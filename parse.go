package sofp

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
)

func (w *Worker) parseDomain(domain string) error {
	log.Println("Starting to parse:", domain)
	isExported, err := w.domainIsExported(domain)
	if isExported || err != nil {
		return err // err will be nil if already exported
	}

	deltaChan, err := w.getDeltaChan(domain)
	if err != nil {
		return err
	}

	partialDir := filepath.Join(w.workingDir, "streams", domain+".partial")
	completedDir := filepath.Join(w.workingDir, "streams", domain)
	os.MkdirAll(partialDir, 0755)
	if err := os.RemoveAll(partialDir); err != nil {
		return err
	}
	if err := os.MkdirAll(partialDir, 0755); err != nil {
		return err
	}

	err = writeDeltasSingleFile(partialDir, deltaChan)
	if err != nil {
		return err
	}

	debug.FreeOSMemory()

	return os.Rename(partialDir, completedDir)

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

func writeDeltasMultiFile(exportDir string, deltaChan chan *Row, fdpool *FDPool) error {

	for delta := range deltaChan {
		// remove everything before the first slash stackoverflow.com/1234 -> 1234
		streamID := strings.Join(strings.Split(delta.StreamID, "/")[1:], "/")
		subDir := ("000" + streamID)[len(streamID):]
		outputDir := filepath.Join(exportDir, subDir)
		err := os.MkdirAll(outputDir, 0755)
		if err != nil {
			return err
		}
		outputFilename := filepath.Join(outputDir, streamID)
		output, err := fdpool.GetFD(outputFilename)
		if err != nil {
			return err
		}
		jsonBytes, err := json.Marshal(delta)
		if err != nil {
			return err
		}
		_, err = output.Write(append(jsonBytes, '\n'))
		if err != nil {
			return err
		}
	}

	return nil
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

func (w *Worker) getDeltaChan(domain string) (chan *Row, error) {

	deltaChan := make(chan *Row)
	go func() {
		allParsers, err := w.getAllParsers(domain)
		defer close(deltaChan)
		defer func() {
			for _, p := range allParsers {
				p.Close()
			}
		}()
		if err != nil {
			return
		}

		lookup := make([]uint32, 100*1000*1000)
		for allParsers[PostsType].Peek() != nil {
			post := allParsers[PostsType].Next()
			if post.PostTypeID == "2" {
				lookup[*post.ID] = uint32(*post.ParentID)
			} else {
				lookup[*post.ID] = uint32(*post.ID)
			}
			for _, deltaType := range DeltaTypeOrder {
				psr := allParsers[deltaType]
				for psr.Peek() != nil && *psr.Peek().PostID <= *post.ID {
					d := psr.Next()
					d.DeltaType = deltaType
					d.StreamID = fmt.Sprintf("%s/%d", domain, lookup[*d.PostID])
					deltaChan <- d
				}
			}
		}
		lookup = []uint32{}
	}()

	return deltaChan, nil

}

func (w *Worker) getAllParsers(domain string) (map[string]*RowsParser, error) {
	allParsers := map[string]*RowsParser{}

	postsPsr, err := w.getXmlReader(domain, PostsType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostsType] = postsPsr

	historyPsr, err := w.getXmlReader(domain, PostHistoryType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostHistoryType] = historyPsr

	commentsPsr, err := w.getXmlReader(domain, CommentsType)
	if err != nil {
		return allParsers, err
	}
	allParsers[CommentsType] = commentsPsr

	linksPsr, err := w.getXmlReader(domain, PostLinksType)
	if err != nil {
		return allParsers, err
	}
	allParsers[PostLinksType] = linksPsr

	votesPsr, err := w.getXmlReader(domain, VotesType)
	if err != nil {
		return allParsers, err
	}
	allParsers[VotesType] = votesPsr

	return allParsers, nil
}

func (w *Worker) getXmlReader(domain string, deltaType string) (*RowsParser, error) {
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
	if err != nil {
		return nil, err
	}
	go func() {
		cmd.Wait()
	}()

	return NewParser(stdout)

}
