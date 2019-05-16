package sofp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type streamWriter struct {
	baseDir  string
	idToFile map[string]*os.File
}

type Delta interface {
	StreamID() string
}

func NewStreamWriter(baseDir string) (*streamWriter, error) {
	return &streamWriter{
		baseDir:  baseDir,
		idToFile: map[string]*os.File{},
	}, nil
}

func (w *streamWriter) Write(d Delta) error {
	streamID := d.StreamID()
	if streamID == "" {
		streamID = "0000nostream"
	}
	f, ok := w.idToFile[streamID]
	var err error
	if !ok {
		//fmt.Println("opening stream:", streamID)
		paddedStream := "00000000" + d.StreamID()
		dir1 := paddedStream[len(paddedStream)-3:]
		fullDirPath := filepath.Join(w.baseDir, dir1)
		fullFilePath := filepath.Join(fullDirPath, streamID)
		os.MkdirAll(fullDirPath, 0755)
		f, err = os.OpenFile(fullFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("cant open file:", err)
			return err
		}
		w.idToFile[streamID] = f
	}

	text, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = f.WriteString(string(text))
	if err != nil {
		fmt.Println("couldnt write to file:", err)
		return err
	}
	_, err = f.WriteString("\n")
	if err != nil {
		return err
	}
	return nil
}

func (w *streamWriter) Shutdown() error {
	for _, f := range w.idToFile {
		f.Close()
	}
	return nil
}
