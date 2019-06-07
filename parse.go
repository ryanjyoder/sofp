package sofp

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/dgraph-io/badger"
	"github.com/ryanjyoder/couchdb"
)

func (w *Worker) parseDomain(domain string) error {
	log.Println("Starting to parse:", domain)

	err := w.getStreamLookup(domain)
	if err != nil {
		return err
	}

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
		streamIDBytes, err := w.getKeyValue(intToBytes(*row.PostID))
		streamID := bytesToInt(streamIDBytes)
		if streamID == 0 || err != nil { // not found, but be a question
			streamID = *row.PostID
		}
		row.Stream = fmt.Sprintf("%d", streamID)
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

func (w *Worker) getStreamLookup(domain string) error {
	log.Println("Preparing lookup for:", domain)
	status, _ := w.getKeyValue([]byte(domain + ":lookup-status"))
	lookupIsBuilt := string(status) == "built"

	if lookupIsBuilt {
		log.Println("lookup alreayd built. Saving some time.")
		return nil
	}

	stdout, err := w.getXmlReader(domain, "Posts")
	if err != nil {
		return err
	}
	defer stdout.Close()
	psr, err := NewParser(stdout)
	if err != nil {
		return err
	}

	for row := psr.Next(); row != nil; row = psr.Next() {
		if row.err != nil {
			fmt.Println("error parsing row:", row.err)
			continue
		}
		if row.PostTypeID == "2" {
			err := w.setKeyValue(intToBytes(*row.ID), intToBytes(*row.ParentID))
			if err != nil {
				return err
			}
		}
	}
	log.Println("finished building lookup")
	w.setKeyValue([]byte(domain+":lookup-status"), []byte("built"))
	return nil
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
	err = cmd.Start()
	go func() {
		cmd.Wait()
	}()
	return stdout, err
}

func (w *Worker) getKeyValue(key []byte) ([]byte, error) {
	var val []byte
	err := w.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		val, err = item.Value()
		return err
	})

	return val, err
}
func (w *Worker) setKeyValue(key, value []byte) error {
	return w.badger.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

func intToBytes(n int) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, int64(n))
	return buf
}

func bytesToInt(b []byte) int {
	x, _ := binary.Varint(b)
	return int(x)
}
