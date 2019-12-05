package sofp

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	FilenameSqlite = "streams.sqlite"
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
	FDPoolSize            int
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
	sites, err := w.getAvailableDomains()
	if err != nil {
		return err
	}

	log.Println("Begin processing")
	wg := sync.WaitGroup{}
	for _, site := range sites {
		wg.Add(1)
		go func(d Site) {
			defer wg.Done()
			fullURLStr := d.Url
			fullURL, _ := url.Parse(fullURLStr)
			domain := fullURL.Host
			w.processDomain(domain, d.TotalQuestions)
		}(site)
		time.Sleep(time.Second)
	}
	wg.Wait()

	return nil
}

func (w *Worker) processDomain(domain string, version string) error {
	err := w.downloadDomain(domain, version)
	if err != nil {
		log.Println("download failed for:", domain, err)
		return nil
	}

	err = w.parseDomain(domain, version)
	if err != nil {
		log.Println("parsing failed for:", domain, err)
		return nil
	}

	return nil
}

func (w *Worker) resetDomain(domain string) error {
	fullpath := filepath.Join(w.workingDir, domain)
	err := os.RemoveAll(fullpath)
	if err != nil {
		return err
	}
	return os.MkdirAll(fullpath, 0755)
}

func GetDefaultConfigs() (WorkerConfigs, error) {
	cfg := WorkerConfigs{}
	if len(os.Args) < 2 {
		return cfg, fmt.Errorf("storage directory not provided")
	}

	// SIMULTANEOUS_DOWNLOAD //
	simDownloadStr, _ := os.LookupEnv("SIMULTANEOUS_DOWNLOAD")
	if simDownloadStr == "" {
		simDownloadStr = "5"
	}
	simDownload, err := strconv.Atoi(simDownloadStr)
	if err != nil {
		return cfg, err
	}
	cfg.SimultaneousDownloads = int64(simDownload)

	// SIMULTANEOUS_PARSE //
	simParseStr, _ := os.LookupEnv("SIMULTANEOUS_PARSE")
	if simParseStr == "" {
		simParseStr = "1"
	}
	simParse, err := strconv.Atoi(simParseStr)
	if err != nil {
		return cfg, err
	}
	cfg.SimultaneousParsers = int64(simParse)

	// FD_POOL_SIZE //
	fdPoolSizeStr, _ := os.LookupEnv("FD_POOL_SIZE")
	if fdPoolSizeStr == "" {
		fdPoolSizeStr = "100"
	}
	fdPoolSize, err := strconv.Atoi(fdPoolSizeStr)
	if err != nil {
		return cfg, err
	}
	cfg.FDPoolSize = fdPoolSize

	// Store directory //
	cfg.StorageDirectory = os.Args[1]

	return cfg, nil
}

const DownloadedFlag = "download-complete"
const ParsedFlag = "parsing-complete"

func (w *Worker) domainFlagSet(domain string, flag string) (bool, error) {
	flagPath := filepath.Join(w.workingDir, domain, flag)
	_, err := os.Stat(flagPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
func (w *Worker) domainSetFlag(domain string, flag string, set bool) error {
	flagPath := filepath.Join(w.workingDir, domain, flag)
	if set {
		f, err := os.Create(flagPath)
		if err != nil {
			return err
		}
		return f.Close()
	}

	return os.Remove(flagPath)
}
