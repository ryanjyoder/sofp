package sofp

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ryanjyoder/couchdb"
)

func (w *Worker) parseDomain(domain string) error {

	lookup, err := w.getStreamLookup(domain)

	fmt.Println("ready to read PostHistory:", len(lookup), err)

	dbName := strings.ReplaceAll(domain, ".", "_")
	resp, err := w.couchClient.Create(dbName)
	if err != nil {
		cErr, ok := err.(*couchdb.Error)
		if !(ok && cErr.StatusCode == 412) {
			log.Fatal(resp, err)
		}
	}
	db := w.couchClient.Use(dbName)
	historyReader, err := w.getXmlReader(domain, "PostHistory")
	if err != nil {
		return err
	}
	defer historyReader.Close()
	psr, err := NewParser(historyReader)
	if err != nil {
		return err
	}

	psr, err = NewParser(historyReader)
	if err != nil {
		return err
	}

	n := 25
	bulk := []couchdb.CouchDoc{}
	for row := psr.Next(); row != nil; row = psr.Next() {
		row.Stream = fmt.Sprintf("%d", lookup[*row.ID])
		bulk = append(bulk, row)
		if len(bulk) >= n {
			_, err := db.Bulk(bulk)
			if err != nil {
				cErr, ok := err.(*couchdb.Error)
				if !ok || cErr.StatusCode != 409 {
					return err
				}
			}
			bulk = []couchdb.CouchDoc{}
		}
	}
	if len(bulk) > 0 {
		db.Bulk(bulk)
	}
	return nil

}

func (w *Worker) getStreamLookup(domain string) ([]int, error) {
	stdout, err := w.getXmlReader(domain, "Posts")
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	psr, err := NewParser(stdout)
	if err != nil {
		return []int{}, err
	}

	lookup := dySlice{}
	for row := psr.Next(); row != nil; row = psr.Next() {
		if row.err != nil {
			fmt.Println("error parsing row:", row.err)
			continue
		}
		if row.PostTypeID == "1" {
			lookup.insert(*row.ID, *row.ID)
		}
		if row.PostTypeID == "2" {
			lookup.insert(*row.ID, *row.ParentID)
		}
	}
	return lookup.s, nil
}

func (w *Worker) getXmlReader(domain string, deltaType string) (io.ReadCloser, error) {
	postZipFilanme := filepath.Join(w.workingDir, "zips", domain+".7z")
	_, err := os.Stat(postZipFilanme)
	// hmmm try with '-Post' postfix
	if err != nil {
		postZipFilanme = filepath.Join(w.workingDir, "zips", domain+"-Posts.7z")
	}

	cmd := exec.Command("7z", "e", "-so", postZipFilanme, "Posts.xml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Start()
	return stdout, nil
}

type dySlice struct {
	s []int
}

func (ds *dySlice) insert(i int, v int) {
	if i >= len(ds.s) {
		newSize := int(1.1*float64(i) + 3)
		newSlice := make([]int, newSize)
		copy(newSlice, ds.s)
		ds.s = newSlice
	}
	ds.s[i] = v

}
