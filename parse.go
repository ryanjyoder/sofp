package sofp

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
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

	err = writeDeltas(partialDir, deltaChan, w.fdPool)
	if err != nil {
		return err
	}
	if err := w.fdPool.CloseAll(); err != nil {
		return err
	}
	debug.FreeOSMemory()

	return os.Rename(partialDir, completedDir)
}

func writeDeltas(exportDir string, deltaChan chan *Row, fdpool *FDPool) error {

	for delta := range deltaChan {
		subDir := ("000" + delta.StreamID)[len(delta.StreamID):]
		outputDir := filepath.Join(exportDir, subDir)
		err := os.MkdirAll(outputDir, 0755)
		if err != nil {
			return err
		}
		outputFilename := filepath.Join(outputDir, delta.StreamID)
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

	postsPsr, err := w.getXmlReader(domain, PostsType)
	if err != nil {
		return nil, err
	}
	historyPsr, err := w.getXmlReader(domain, PostHistoryType)
	if err != nil {
		return nil, err
	}

	commentsPsr, err := w.getXmlReader(domain, CommentsType)
	if err != nil {
		return nil, err
	}

	deltaChan := make(chan *Row)
	go func() {
		defer close(deltaChan)
		lookup := make([]uint32, 100*1000*1000)
		for post := postsPsr.Next(); post != nil; post = postsPsr.Next() {
			if post.PostTypeID == "2" {
				lookup[*post.ID] = uint32(*post.ParentID)
			} else {
				lookup[*post.ID] = uint32(*post.ID)
			}
			for historyPsr.Peek() != nil && *historyPsr.Peek().PostID <= *post.ID {
				d := historyPsr.Next()
				d.DeltaType = PostHistoryType
				d.StreamID = fmt.Sprint(lookup[*d.PostID])
				deltaChan <- d
			}
			for commentsPsr.Peek() != nil && *commentsPsr.Peek().PostID <= *post.ID {
				d := commentsPsr.Next()
				d.DeltaType = CommentsType
				d.StreamID = fmt.Sprint(lookup[*d.PostID])
				deltaChan <- d
			}
		}
		lookup = []uint32{}
	}()

	return deltaChan, nil

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
