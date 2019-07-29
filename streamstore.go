package sofp

import (
	"database/sql"
	"encoding/json"

	sqlite3 "github.com/mattn/go-sqlite3"
)

type StreamStore struct {
	db *sql.DB
}

type Delta interface {
	GetStreamID() string
	GetID() string
}

var (
	dbSetupQueries = []string{`CREATE TABLE IF NOT EXISTS deltas (
		ordering INTEGER PRIMARY KEY AUTOINCREMENT,
		id text UNIQUE,
		streamID text,
		msg TEXT
	)`, `CREATE INDEX IF NOT EXISTS
		streamIDIndex ON
		deltas(streamID)
	`, `CREATE TABLE IF NOT EXISTS progress (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task TEXT,
		finished t TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		length INTEGER)`}
)

func NewStreamStore(dbFilepath string) (*StreamStore, error) {
	database, err := sql.Open("sqlite3", dbFilepath)
	if err != nil {
		return nil, err
	}

	for _, q := range dbSetupQueries {
		statement, err := database.Prepare(q)
		if err != nil {
			return nil, err
		}
		_, err = statement.Exec()
		if err != nil {
			return nil, err
		}
	}

	return &StreamStore{
		db: database,
	}, nil
}

// Write returns if the if the delta is new
func WriteDeltaToDB(d Delta, tx *sql.Tx) (bool, error) {
	text, err := json.Marshal(d)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(`
	INSERT INTO deltas
		(id, streamID, msg)
	VALUES 
		(?, ?, ?)
	`, d.GetID(), d.GetStreamID(), string(text))

	if err == nil {
		return true, nil // new delta inserted

	}
	if sqlErr, ok := err.(sqlite3.Error); ok {
		if sqlErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey {
			return false, nil // not new but no error
		}
	}
	return false, err

}

func (w *StreamStore) GetStreamDeltas(id string) ([]*Row, error) {
	rows, err := w.db.Query(`
	SELECT msg FROM deltas WHERE streamID = ? order by ordering
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deltas := []*Row{}
	for rows.Next() {
		msg := ""
		delta := &Row{}
		err := rows.Scan(&msg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(msg), delta)
		if err != nil {
			return nil, err
		}
		deltas = append(deltas, delta)
	}

	return deltas, nil
}

func (w *StreamStore) ListStreamIDs() (*sql.Rows, error) {
	return w.db.Query(`
	SELECT streamID FROM deltas group by streamID
	`)
}

func (w *StreamStore) LastDelta() (string, error) {
	rows, err := w.db.Query(`
	SELECT id FROM deltas where ordering = (select max(ordering) from deltas)
	`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var deltaID string
	for rows.Next() {
		err := rows.Scan(&deltaID)
		if err != nil {
			return "", err
		}
		return deltaID, nil
	}

	return "", nil
}

func (w *StreamStore) Begin() (*sql.Tx, error) {
	return w.db.Begin()
}
