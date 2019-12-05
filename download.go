package sofp

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cavaliercoder/grab"
)

func (w *Worker) downloadDomain(domain string, version string) error {
	downloaded, err := w.domainFlagSet(domain+"/"+version, DownloadedFlag)
	if err != nil {
		return err
	}
	if downloaded {
		return nil
	}

	w.downloadSemephore.Acquire(context.TODO(), 1)
	defer w.downloadSemephore.Release(1)

	filenames := get7zFilenames(domain)
	for _, filename := range filenames {
		output := filepath.Join(w.workingDir, domain, version, filename)
		downloadUrl := "https://archive.org/download/stackexchange/" + filename
		_, err := grab.Get(output, downloadUrl)
		if err != nil {
			return err
		}
	}

	return w.domainSetFlag(domain+"/"+version, DownloadedFlag, true)
}

func (w *Worker) getAvailableDomains() ([]Site, error) {
	log.Println("Working Directory:", w.workingDir)
	err := os.MkdirAll(w.workingDir, 0755)
	if err != nil {
		return nil, err
	}

	newSitesPath := filepath.Join(w.workingDir, "new-Sites.xml")
	_, err = grab.Get(newSitesPath, "https://archive.org/download/stackexchange/Sites.xml")
	if err != nil {
		return nil, err
	}

	newSitesHash := hashFile(newSitesPath)
	if newSitesHash == "" {
		return nil, fmt.Errorf("couldn't get new-Sites.xml hash")
	}

	sitesPath := filepath.Join(w.workingDir, "Sites.xml")
	oldSiteshash := hashFile(sitesPath)

	if newSitesHash != oldSiteshash {
		log.Println("New updates available")
		if err != nil {
			return nil, err
		}
		// if the hash has actually change, opposed to Sites.xml never existing
		// we're going to wait a few hours just in case the upload is still in progress
		// we could replace this with an actually check of each file's modification date.
		// but honestly you already waited 3 months, what's 12 morre hours
		if oldSiteshash != "" {
			log.Println("pausing to make sure the complete upload is finished")
			time.Sleep(12 * time.Hour)
		}
	}

	os.Rename(newSitesPath, sitesPath)
	buf := bytes.NewBuffer([]byte(sitesPath))
	domains, err := GetDomainsFromSitesXml(buf)

	return domains, err
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
