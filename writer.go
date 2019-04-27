package sofp

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type streamWriter struct {
	baseDir string
}

type Delta interface {
	StreamID() string
}

func NewStreamWriter(baseDir string) (*streamWriter, error) {
	return &streamWriter{
		baseDir: baseDir,
	}, nil
}

func (w *streamWriter) Write(d Delta) error {
	paddedStream := d.StreamID() + "0000"
	dir1 := paddedStream[:2]
	dir2 := paddedStream[2:4]
	fullDirPath := filepath.Join(w.baseDir, dir1, dir2)
	fullFilePath := filepath.Join(fullDirPath, d.StreamID())
	os.MkdirAll(fullDirPath, 0755)
	f, err := os.OpenFile(fullFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer f.Close()

	text, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = f.WriteString(string(text))
	if err != nil {
		return err
	}
	_, err = f.WriteString("\n")
	if err != nil {
		return err
	}
	return nil
}
