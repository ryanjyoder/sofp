package sofp

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sync/semaphore"
)

type Worker struct {
	workingDir        string
	downloadSemephore *semaphore.Weighted
	parseSemephore    *semaphore.Weighted
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
	return &Worker{
		workingDir:        workingDir,
		downloadSemephore: semaphore.NewWeighted(configs.SimultaneousDownloads),
	}, nil
}

func (w *Worker) Run() error {
	completedDomains, err := w.getDownloaded()
	if err != nil {
		return err
	}

	log.Println("Begin parsing")
	for domain := range completedDomains {
		fmt.Println("downloaded:", domain)
	}
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
