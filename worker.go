package sofp

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ryanjyoder/couchdb"
	"golang.org/x/sync/semaphore"
)

type Worker struct {
	workingDir        string
	downloadSemephore *semaphore.Weighted
	parseSemephore    *semaphore.Weighted
	couchClient       couchdb.ClientService
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

	return &Worker{
		workingDir:        workingDir,
		downloadSemephore: semaphore.NewWeighted(configs.SimultaneousDownloads),
		parseSemephore:    semaphore.NewWeighted(configs.SimultaneousParsers),
		couchClient:       client,
	}, nil
}

func (w *Worker) Run() error {
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
	if len(os.Args) < 2 {
		return WorkerConfigs{}, fmt.Errorf("storage directory not provided")
	}
	couchdbHost, ok := os.LookupEnv("COUCHDB_HOST")
	if !ok {
		return WorkerConfigs{}, fmt.Errorf("env COUCHDB_HOST not set")
	}

	couchdbUser, ok := os.LookupEnv("COUCHDB_USER")
	if !ok {
		return WorkerConfigs{}, fmt.Errorf("env COUCHDB_USER not set")
	}

	couchdbPass, ok := os.LookupEnv("COUCHDB_PASS")
	if !ok {
		return WorkerConfigs{}, fmt.Errorf("env COUCHDB_HOST not set")
	}

	return WorkerConfigs{
		StorageDirectory:      os.Args[1],
		SimultaneousDownloads: 10,
		SimultaneousParsers:   int64(runtime.NumCPU()),
		CouchDBHost:           couchdbHost,
		CouchDBUser:           couchdbUser,
		CouchDBPass:           couchdbPass,
	}, nil
}
