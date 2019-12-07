package sofp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

func ListLookupBuiltDomains(rootDir string) ([]string, error) {
	return ListDomainsWithFlag(rootDir, "lookup-built")
}

func BuildSqlite(ctx context.Context, rootDir string, domain string) (string, error) {
	version, err := os.Readlink(filepath.Join(rootDir, domain, "current"))
	if err != nil {
		return version, err
	}

	deltaChan, err := getDeltaChanFromArchive(ctx, rootDir, domain, version)
	if err != nil {
		return "", err
	}

	fmt.Println("got parsers. writing deltas")
	sqlitePath := filepath.Join(rootDir, domain, version, FilenameSqlite)
	err = writeDeltasToSqlite(sqlitePath, deltaChan)
	if err != nil {
		return "", err
	}

	cmd := exec.Command("gzip", "-k", "-f", sqlitePath)
	err = cmd.Run()
	if err != nil {
		return "", err
	}

	return version, nil
}

func getDeltaChanFromArchive(ctx context.Context, rootDir string, domain string, version string) (chan *Row, error) {
	allParsers, err := getAllParsers(rootDir, domain, version)
	if err != nil {
		return nil, err
	}

	lookupFilepath := filepath.Join(rootDir, domain, version, FilenameLookupDB)
	lookup, err := NewPostIDLookup(lookupFilepath)
	if err != nil {
		return nil, err
	}

	maxPostID := lookup.MaxPostID()

	deltaChan := make(chan *Row)
	go func() {
		defer close(deltaChan)
		defer func() {
			for _, p := range allParsers {
				p.Close()
			}
		}()

		for postID := 0; postID <= maxPostID; postID++ {
			for _, deltaType := range DeltaTypeOrder {
				psr := allParsers[deltaType]
				for psr.Peek() != nil && *psr.Peek().PostID <= postID {
					d := psr.Next()
					d.DeltaType = deltaType
					d.StreamID = fmt.Sprintf("%s/%d", domain, lookup.Get(*d.PostID))
					deltaChan <- d
				}
			}
		}

	}()

	return deltaChan, nil

}

func SqliteIsBuilt(rootDir string, domain string) (bool, error) {
	ok, err := flagIsSet(rootDir, domain, "sqlite-built")
	return ok, err
}

func SetSqliteBuilt(rootDir string, domain string, version string) error {
	return setFlag(rootDir, domain, version, "sqlite-built")
}

type PostIDLookup struct {
	db *bolt.DB
}

func NewPostIDLookup(boltPath string) (*PostIDLookup, error) {
	db, err := bolt.Open(boltPath, 0666, nil)

	return &PostIDLookup{
		db: db,
	}, err
}

func (l *PostIDLookup) Get(id int) int {
	parentID := 0
	l.db.View(func(tx *bolt.Tx) error {
		lookup := tx.Bucket([]byte("lookup"))
		parentID = btoi(lookup.Get(itob(id)))
		return nil
	})
	return parentID
}

func (l *PostIDLookup) Close() error {
	return l.db.Close()
}

func (l *PostIDLookup) MaxPostID() int {
	maxID := 0
	l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lookup"))
		if b == nil {
			return nil
		}

		b.ForEach(func(k, v []byte) error {
			id := btoi(k)
			if id > maxID {
				maxID = id
			}
			return nil
		})
		return nil
	})
	return maxID
}
