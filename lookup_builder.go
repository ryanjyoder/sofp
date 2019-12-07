package sofp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	bolt "go.etcd.io/bbolt"
)

func ListDownloadedDomains(rootDir string) ([]string, error) {
	return ListDomainsWithFlag(rootDir, "download-complete")
}
func ListDomainsWithFlag(rootDir string, flag string) ([]string, error) {
	prefix, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}

	domains := []string{}
	re := regexp.MustCompile(prefix + "/([^/]+)/current")
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		absPath, _ := filepath.Abs(path)
		matchSlice := re.FindStringSubmatch(absPath)
		if len(matchSlice) < 2 {
			return nil
		}
		if _, err := os.Stat(filepath.Join(path, flag)); err != nil {
			fmt.Println("no download flag", filepath.Join(path, flag))
			return nil
		}
		domains = append(domains, matchSlice[1])
		return nil
	})

	return domains, err
}

func PostIDLookupIsBuilt(rootDir string, domain string) (bool, error) {
	ok, err := flagIsSet(rootDir, domain, "lookup-built")
	return ok, err
}

func BuildPostIDLookup(ctx context.Context, rootDir string, domain string) (string, error) {
	version, err := os.Readlink(filepath.Join(rootDir, domain, "current"))
	if err != nil {
		return version, err
	}

	zipFilename := filepath.Join(rootDir, domain, version, domain+".7z")
	if domainIsMultiArchive[domain] {
		zipFilename = filepath.Join(rootDir, domain, version, domain+"-Posts.7z")
	}
	xmlFilename := "Posts.xml"

	xmlReader, err := getXmlIOReader(zipFilename, xmlFilename)
	if err != nil {
		return version, err
	}
	postParser, err := NewParser(xmlReader)
	if err != nil {
		return version, err
	}

	boltPath := filepath.Join(rootDir, domain, version, FilenameLookupDB)
	db, err := bolt.Open(boltPath, 0666, nil)
	if err != nil {
		return version, err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("lookup"))
		return err

	})
	if err != nil {
		return version, err
	}

	for postParser.Peek() != nil {
		p := postParser.Next()
		go func(post Row) {
			err = db.Batch(func(tx *bolt.Tx) error {
				lookup := tx.Bucket([]byte("lookup"))
				if post.PostTypeID == "2" {
					lookup.Put(itob(*post.ID), itob(*post.ParentID))
					// lookup[*post.ID] = uint32(*post.ParentID)
				} else {
					lookup.Put(itob(*post.ID), itob(*post.ID))
					//lookup[*post.ID] = uint32(*post.ID)
				}

				return nil
			})
		}(*p)

	}

	return version, nil
}

func SetLookupBuilt(rootDir string, domain string, version string) error {
	return setFlag(rootDir, domain, version, "lookup-built")
}

func getXmlIOReader(zipFilename string, xmlFilename string) (io.ReadCloser, error) {
	cmd := exec.Command("7z", "e", "-so", zipFilename, xmlFilename)
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

	return stdout, nil
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// btoi returns reverses itob
func btoi(v []byte) int {
	if len(v) < 1 {
		return 0
	}
	return int(binary.BigEndian.Uint64(v))
}
