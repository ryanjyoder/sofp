package sofp

import (
	"encoding/json"
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

	db, err := w.prepareDB(domain)
	if err != nil {
		return err
	}

	err = w.parseToCouch(domain, lookup, db)
	if err != nil {
		return err
	}

	err = w.exportCouchStreams(db, domain)

	return err
}

func (w *Worker) domainIsExported(domain string) (bool, error) {
	streamDir := filepath.Join(w.workingDir, "streams", domain)
	statInfo, err := os.Stat(streamDir)
	if err == nil && statInfo.IsDir() {
		return true, nil
	}
	if err == nil && !statInfo.IsDir() {
		return false, fmt.Errorf("not a directory:", streamDir)
	}
	if os.IsNotExist((err)) {
		return false, nil
	}
	return false, err
}

func (w *Worker) exportCouchStreams(db couchdb.DatabaseService, domain string) error {
	log.Println("exporting from couchdb:", domain)
	isExport, err := w.domainIsExported(domain)
	if err != nil {
		return err
	}
	if isExport {
		return nil
	}
	exportDir := filepath.Join(w.workingDir, "streams", domain+".partial")
	os.MkdirAll(exportDir, 0755)
	err = os.RemoveAll(exportDir)
	if err != nil {
		return err
	}

	//exportDir := filepath.Join(w.workingDir, "streams", domain+".partial")
	itr, err := NewQueryIterator(db, "streams", "byStreamID")
	if err != nil {
		return err
	}

	var streamID, deltaType string
	var output io.WriteCloser
	defer func() {
		if output != nil {
			output.Close()
		}
	}()
	row, err := itr.Next()
	if err != nil {
		return err
	}
	for row != nil {
		nextStreamID, nextDeltaType, err := extractKeys(row.Key)
		if err != nil {
			return err
		}
		if nextStreamID != streamID || nextDeltaType != deltaType {
			if output != nil {
				output.Close()
			}
			streamID, deltaType = nextStreamID, nextDeltaType
			subDir := string("000" + streamID)[len(streamID):]
			outputDir := filepath.Join(exportDir, subDir, streamID)
			err = os.MkdirAll(outputDir, 0755)
			if err != nil {
				return err
			}
			outputFilename := filepath.Join(outputDir, deltaType)
			output, err = os.Create(outputFilename)
			if err != nil {
				return err
			}
		}
		jsonBytes, err := json.Marshal(row.Value)
		if err != nil {
			return err
		}
		_, err = output.Write(append(jsonBytes, '\n'))
		if err != nil {
			return err
		}

		row, err = itr.Next()
		if err != nil {
			return fmt.Errorf("Error reading rows:", err)
		}
	}

	completedDir := filepath.Join(w.workingDir, "streams", domain)
	return os.Rename(exportDir, completedDir)
}

func (w *Worker) parseToCouch(domain string, lookup []int, db couchdb.DatabaseService) error {

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
		streamID := lookup[*row.PostID]
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

func (w *Worker) getStreamLookup(domain string) ([]int, error) {
	log.Println("Preparing lookup for:", domain)

	stdout, err := w.getXmlReader(domain, "Posts")
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	psr, err := NewParser(stdout)
	if err != nil {
		return nil, err
	}

	lookup := make([]int, 75*1000*1000)

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
	err = cmd.Start()
	go func() {
		cmd.Wait()
	}()
	return stdout, err
}

/*

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
*/
