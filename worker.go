package sofp

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

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
}

func NewWorker(configs WorkerConfigs) (*Worker, error) {
	workingDir, err := filepath.Abs(configs.StorageDirectory)
	if err != nil {
		return nil, err
	}

	return &Worker{
		workingDir:        workingDir,
		downloadSemephore: semaphore.NewWeighted(configs.SimultaneousDownloads),
		parseSemephore:    semaphore.NewWeighted(configs.SimultaneousParsers),
	}, nil
}

func (w *Worker) Run() error {
	//defer w.badger.Close()
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
