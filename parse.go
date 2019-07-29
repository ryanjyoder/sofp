package sofp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
)

func (w *Worker) parseDomain(domain string, version string) error {

	isExported, err := w.domainFlagSet(domain+"/"+version, ParsedFlag)
	if isExported || err != nil {
		return err // err will be nil if already exported
	}

	w.parseSemephore.Acquire(context.TODO(), 1)
	defer w.parseSemephore.Release(1)
	log.Println("Starting to parse:", domain)

	deltaChan, err := w.getDeltaChan(domain, version)
	if err != nil {
		return err
	}

	sqlitePath := filepath.Join(w.workingDir, domain, version, FilenameSqlite)
	err = writeDeltasToSqlite(sqlitePath, deltaChan)
	if err != nil {
		return err
	}

	cmd := exec.Command("gzip", "-k", "-f", sqlitePath)
	err = cmd.Run()
	if err != nil {
		return err
	}

	return w.domainSetFlag(domain+"/"+version, ParsedFlag, true)

}

func writeDeltasToSqlite(sqliteFile string, deltaChan chan *Row) error {
	store, err := NewStreamStore(sqliteFile)
	if err != nil {
		return err
	}

	lastDelta, err := store.LastDelta()

	tx, err := store.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// This indicates whether we've read through the channel to new deltas not in the db
	// if the last delta is empty then the db is empty and the stream is ready to start inserting
	streamIsReset := lastDelta == ""

	for delta := range deltaChan {
		// keep reading from the chan until we see the last delta in the db. Then we'll starting inserting
		if !streamIsReset {
			streamIsReset = delta.GetID() == lastDelta
			continue
		}

		_, err := WriteDeltaToDB(delta, tx)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	err = store.db.Close()
	if err != nil {
		return err
	}
	fmt.Println("parsing complete:", sqliteFile)

	return nil
}

func writeDeltasSingleFile(exportDir string, deltaChan chan *Row) error {
	outputFile := filepath.Join(exportDir, "unified-stream")
	fd, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	for delta := range deltaChan {
		jsonBytes, err := json.Marshal(delta)
		if err != nil {
			return err
		}
		_, err = fd.Write(append(jsonBytes, '\n'))
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) getDeltaChan(domain string, version string) (chan *Row, error) {
	allParsers, err := w.getAllParsers(domain, version)
	if err != nil {
		return nil, err
	}

	deltaChan := make(chan *Row)
	go func() {
		defer close(deltaChan)
		defer func() {
			for _, p := range allParsers {
				p.Close()
			}
		}()

		lookup := make([]uint32, 100*1000*1000)
		maxPostID := 0
		for allParsers[PostsType].Peek() != nil {
			post := allParsers[PostsType].Next()
			if post.PostTypeID == "2" {
				lookup[*post.ID] = uint32(*post.ParentID)
			} else {
				lookup[*post.ID] = uint32(*post.ID)
			}
			if *post.ID > maxPostID {
				maxPostID = *post.ID
			}
		}

		for postID := 0; postID <= maxPostID; postID++ {
			for _, deltaType := range DeltaTypeOrder {
				psr := allParsers[deltaType]
				for psr.Peek() != nil && *psr.Peek().PostID <= postID {
					d := psr.Next()
					d.DeltaType = deltaType
					d.StreamID = fmt.Sprintf("%s/%d", domain, lookup[*d.PostID])
					deltaChan <- d
				}
			}
		}
		lookup = []uint32{} // this very large array needs to be deallocated before the next one gets created
		debug.FreeOSMemory()
	}()

	return deltaChan, nil

}

func (w *Worker) getAllParsers(domain string, version string) (map[string]*RowsParser, error) {
	allParsers := map[string]*RowsParser{}

	postsPsr, err := w.getXmlReader(domain, version, PostsType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostsType] = postsPsr

	historyPsr, err := w.getXmlReader(domain, version, PostHistoryType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostHistoryType] = historyPsr

	commentsPsr, err := w.getXmlReader(domain, version, CommentsType)
	if err != nil {
		return allParsers, err
	}
	allParsers[CommentsType] = commentsPsr

	linksPsr, err := w.getXmlReader(domain, version, PostLinksType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostLinksType] = linksPsr

	votesPsr, err := w.getXmlReader(domain, version, VotesType)
	if err != nil {
		return allParsers, err
	}
	allParsers[VotesType] = votesPsr

	return allParsers, nil
}

func (w *Worker) getXmlReader(domain string, version string, deltaType string) (*RowsParser, error) {
	postZipFilanme := filepath.Join(w.workingDir, domain, version, domain+".7z")
	_, err := os.Stat(postZipFilanme)
	// hmmm try with '-Post' postfix
	if err != nil {
		postZipFilanme = filepath.Join(w.workingDir, domain, version, domain+"-"+deltaType+".7z")
	}

	cmd := exec.Command("7z", "e", "-so", postZipFilanme, deltaType+".xml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	go func() {
		cmd.Wait()
	}()

	return NewParser(stdout)

}
