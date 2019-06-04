package sofp

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ryanjyoder/couchdb"
)

func (w *Worker) parseDomain(domain string) error {

	lookup, err := w.getStreamLookup(domain)
	if err != nil {
		return err
	}
	if len(lookup) < 1 {
		return fmt.Errorf("error getting postiID lookup")
	}
	fmt.Println("ready to read PostHistory:", len(lookup), err)

	dbName := strings.ReplaceAll(domain, ".", "_")
	_, err = w.couchClient.Create(dbName)
	if err != nil {
		cErr, ok := err.(*couchdb.Error)
		if !(ok && cErr.StatusCode == 412) {
			return err
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
		row.DeltaType = "PostHistory"
		row.DeltaID = row.GetID()
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
		_, err := db.Bulk(bulk)
		return err
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
		postZipFilanme = filepath.Join(w.workingDir, "zips", domain+"-"+deltaType+".7z")
	}

	cmd := exec.Command("7z", "e", "-so", postZipFilanme, deltaType+".xml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	return stdout, cmd.Start()
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
