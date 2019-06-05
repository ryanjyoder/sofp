package sofp

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/ryanjyoder/couchdb"
)

func (w *Worker) parseDomain(domain string) error {
	log.Println("Starting to parse:", domain)

	lookup, err := w.getStreamLookup(domain)
	if err != nil {
		return err
	}
	if len(lookup) < 1 {
		return fmt.Errorf("error getting postiID lookup")
	}
	log.Println("Loaded stream lookup table:", len(lookup), err)

	db, err := w.prepareDB(domain)
	if err != nil {
		return err
	}
	historyReader, err := w.getXmlReader(domain, PostHistoryType)
	if err != nil {
		return err
	}
	defer historyReader.Close()
	psr, err := NewParser(historyReader)
	if err != nil {
		return err
	}
	lastSeenID, err := getLastSeenID(db, PostHistoryType)
	if err != nil {
		return err
	}
	if lastSeenID != 0 {
		log.Println("resetting parser to checkpoint:", lastSeenID)
	}
	n := 25
	bulk := []couchdb.CouchDoc{}
	for row := psr.Next(); row != nil; row = psr.Next() {
		if lastSeenID >= *row.ID {
			if lastSeenID == *row.ID {
				log.Println("ok reset. begining insert")
			}
			continue
		}
		row.Stream = fmt.Sprintf("%d", lookup[*row.PostID])
		row.DeltaType = PostHistoryType
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

func (w *Worker) getStreamLookup(domain string) (map[int]int, error) {
	stdout, err := w.getXmlReader(domain, "Posts")
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	psr, err := NewParser(stdout)
	if err != nil {
		return map[int]int{}, err
	}

	lookup := map[int]int{}
	for row := psr.Next(); row != nil; row = psr.Next() {
		if row.err != nil {
			fmt.Println("error parsing row:", row.err)
			continue
		}
		if row.PostTypeID == "1" {
			lookup[*row.ID] = *row.ID
		}
		if row.PostTypeID == "2" {
			lookup[*row.ID] = *row.ParentID
		}
	}
	return lookup, nil
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
