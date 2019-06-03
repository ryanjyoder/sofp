package sofp

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/cavaliercoder/grab"
)

func (w *Worker) getDownloaded() (chan string, error) {

	err := w.prepareDownloads()
	if err != nil {
		return nil, err
	}

	domainsChan := make(chan string)

	go func() {
		defer close(domainsChan)
		wg := sync.WaitGroup{}
		domains, _ := getDomains(filepath.Join(w.workingDir, "Sites.xml"))
		for _, domain := range domains {
			w.downloadSemephore.Acquire(context.TODO(), 1)
			wg.Add(1)
			go func(d string) {
				defer w.downloadSemephore.Release(1)
				defer wg.Done()
				err := w.downloadDomain(d)
				if err != nil {
					log.Println("download failed for:", d)
					return
				}
				domainsChan <- d
			}(domain)
		}
		wg.Wait()
	}()

	return domainsChan, nil

}

func (w *Worker) downloadDomain(domain string) error {
	filenames := get7zFilenames(domain)
	for _, filename := range filenames {
		output := filepath.Join(w.workingDir, "zips", filename)
		downloadUrl := "https://archive.org/download/stackexchange/" + filename
		_, err := grab.Get(output, downloadUrl)
		if err != nil {
			return err
		}
	}
	return nil
}

// prepareDownload will clear old zip files if there are new versions
// and fetch the latest Sites.xml file
func (w *Worker) prepareDownloads() error {
	log.Println("Working Directory:", w.workingDir)
	err := os.MkdirAll(w.workingDir, 0755)
	if err != nil {
		return err
	}

	newSitesPath := filepath.Join(w.workingDir, "new-Sites.xml")
	_, err = grab.Get(newSitesPath, "https://archive.org/download/stackexchange/Sites.xml")
	if err != nil {
		return err
	}

	newSitesHash := hashFile(newSitesPath)
	if newSitesHash == "" {
		return fmt.Errorf("couldn't get new-Sites.xml hash")
	}

	sitesPath := filepath.Join(w.workingDir, "Sites.xml")
	oldSiteshash := hashFile(sitesPath)

	zipsDir := filepath.Join(w.workingDir, "zips")
	if newSitesHash != oldSiteshash {
		log.Println("New updates available")
		err := os.RemoveAll(zipsDir)
		if err != nil {
			return err
		}
	}
	os.MkdirAll(zipsDir, 0755)
	os.Rename(newSitesPath, sitesPath)

	return nil
}

func hashFile(filename string) string {

	f, err := os.Open(filename)
	if err != nil {
		return ""
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func get7zFilenames(domain string) []string {
	if domain == "stackoverflow.com" {
		return []string{
			domain + "-Badges.7z",
			domain + "-Comments.7z",
			domain + "-PostHistory.7z",
			domain + "-PostLinks.7z",
			domain + "-Posts.7z",
			domain + "-Tags.7z",
			domain + "-Users.7z",
			domain + "-Votes.7z",
		}
	}

	return []string{domain + ".7z"}

}
