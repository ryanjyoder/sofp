package sofp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type streamWriter struct {
	baseDir string
	db      *sql.DB
}

type Delta interface {
	StreamID() string
}

func NewStreamWriter(baseDir string) (*streamWriter, error) {

	workingDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	os.MkdirAll(workingDir, 0755)
	dbFilepath := filepath.Join(workingDir, "streams.sqlite")
	database, err := sql.Open("sqlite3", dbFilepath)
	if err != nil {
		return nil, err
	}

	statement, err := database.Prepare(`
		CREATE TABLE IF NOT EXISTS deltas (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			streamID text,
			msg TEXT
		)`)
	if err != nil {
		return nil, err
	}
	_, err = statement.Exec()
	if err != nil {
		return nil, err
	}
	return &streamWriter{
		baseDir: baseDir,
		db:      database,
	}, nil
}

func (w *streamWriter) Write(d Delta) error {
	text, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = w.db.Exec(`
	INSERT INTO deltas
		(streamID, msg)
	VALUES 
		(?, ?)
`, d.StreamID(), string(text))
	if err != nil {
		fmt.Println("error inserting:", err.Error())
		return err
	}
	return nil
}

func (w *streamWriter) ExportStreams(dir string) error {
	fmt.Println("exporting streams")
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)
	w.db.Exec(`CREATE INDEX streamid_idx on deltas(streamID)`)
	resp, err := w.db.Query(`SELECT DISTINCT streamID from deltas`)
	if err != nil {
		return err
	}
	resp.Close()
	for resp.Next() {
		streamID := ""
		resp.Scan(&streamID)
		paddedStream := "00000000" + streamID
		dir1 := paddedStream[len(paddedStream)-3:]
		fullDirPath := filepath.Join(dir, dir1)
		fullFilePath := filepath.Join(fullDirPath, streamID)
		os.MkdirAll(fullDirPath, 0755)

		rows, err := w.db.Query(`SELECT msg streamID FROM deltas WHERE streamID=? ORDER BY id`, streamID)
		if err != nil {
			return err
		}

		err = writeRows(fullFilePath, rows)
		rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
func writeRows(fullFilePath string, rows *sql.Rows) error {

	f, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("cant open file:", err)
		return err
	}
	defer f.Close()

	for rows.Next() {
		msg := ""
		rows.Scan(&msg)
		f.WriteString(msg + "\n")
	}

	return nil
}
