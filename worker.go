package sofp

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/ryanjyoder/couchdb"
	"golang.org/x/sync/semaphore"
)

type Worker struct {
	workingDir        string
	downloadSemephore *semaphore.Weighted
	parseSemephore    *semaphore.Weighted
	couchClient       couchdb.ClientService
	badger            *badger.DB
}

type WorkerConfigs struct {
	StorageDirectory      string
	SimultaneousDownloads int64
	SimultaneousParsers   int64
	CouchDBHost           string
	CouchDBUser           string
	CouchDBPass           string
}

func NewWorker(configs WorkerConfigs) (*Worker, error) {
	workingDir, err := filepath.Abs(configs.StorageDirectory)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(configs.CouchDBHost)
	if err != nil {
		return nil, err
	}
	// create a new client
	client, err := couchdb.NewAuthClient(configs.CouchDBUser, configs.CouchDBPass, u)
	if err != nil {
		return nil, err
	}

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = filepath.Join(workingDir, "badger")
	opts.ValueDir = opts.Dir
	opts.MaxTableSize = 1 << 20
	opts.NumMemtables = 1
	opts.NumCompactors = 1
	opts.TableLoadingMode = options.MemoryMap

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Worker{
		workingDir:        workingDir,
		downloadSemephore: semaphore.NewWeighted(configs.SimultaneousDownloads),
		parseSemephore:    semaphore.NewWeighted(configs.SimultaneousParsers),
		couchClient:       client,
		badger:            db,
	}, nil
}

func (w *Worker) Run() error {
	defer w.badger.Close()
	for {
		err := w.singleRun()
		if err != nil {
			log.Println("Error during run:", err)
		}
		time.Sleep(24 * time.Hour)
	}
}

func (w *Worker) singleRun() error {
	completedDomains, err := w.getDownloaded()
	if err != nil {
		return err
	}

	log.Println("Begin parsing")
	wg := sync.WaitGroup{}
	for domain := range completedDomains {
		w.parseSemephore.Acquire(context.TODO(), 1)
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			defer w.parseSemephore.Release(1)
			err := w.parseDomain(d)
			if err != nil {
				log.Println("Error parsing domain:", d, err)
			}
		}(domain)
	}
	wg.Wait()
	log.Println("finished this run")
	return nil
}

func GetDefaultConfigs() (WorkerConfigs, error) {
	cfg := WorkerConfigs{}
	if len(os.Args) < 2 {
		return cfg, fmt.Errorf("storage directory not provided")
	}
	couchdbHost, ok := os.LookupEnv("COUCHDB_HOST")
	if !ok {
		return cfg, fmt.Errorf("env COUCHDB_HOST not set")
	}
	cfg.CouchDBHost = couchdbHost

	couchdbUser, ok := os.LookupEnv("COUCHDB_USER")
	if !ok {
		return cfg, fmt.Errorf("env COUCHDB_USER not set")
	}
	cfg.CouchDBUser = couchdbUser

	couchdbPass, ok := os.LookupEnv("COUCHDB_PASS")
	if !ok {
		return cfg, fmt.Errorf("env COUCHDB_HOST not set")
	}
	cfg.CouchDBPass = couchdbPass

	simDownloadStr, _ := os.LookupEnv("SIMULTANEOUS_DOWNLOAD")
	if simDownloadStr == "" {
		simDownloadStr = "10"
	}
	simDownload, err := strconv.Atoi(simDownloadStr)
	if err != nil {
		return cfg, err
	}
	cfg.SimultaneousDownloads = int64(simDownload)

	simParseStr, _ := os.LookupEnv("SIMULTANEOUS_PARSE")
	if simParseStr == "" {
		simParseStr = "1"
	}
	simParse, err := strconv.Atoi(simParseStr)
	if err != nil {
		return cfg, err
	}
	cfg.SimultaneousParsers = int64(simParse)
	cfg.StorageDirectory = os.Args[1]

	return cfg, nil
}
