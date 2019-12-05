package sofp

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
)

func ListDownloadedDomains(rootDir string) ([]string, error) {
	prefix, err := filepath.Abs(filepath.Join(rootDir, "current"))
	if err != nil {
		return nil, err
	}

	domains := []string{}
	re := regexp.MustCompile(prefix + "/([^/]+)/download-complete")
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		domainSlice := re.FindStringSubmatch(path)
		domains = append(domains, domainSlice...)
		return nil
	})

	return domains, err
}

func PostIDLookupIsBuilt(rootDir string, domain string) (bool, error) {
	ok, err := flagIsSet(rootDir, domain, "lookup-built")
	return ok, err
}

func BuildPostIDLookup(ctx context.Context, rootDir string, domain string) error {
	// TODO: find version and use instead of current to avoid any race conditions

	zipFilename := filepath.Join(rootDir, domain, "current", domain+".7z")
	if domainIsMultiArchive[domain] {
		zipFilename = filepath.Join(rootDir, domain, "current", domain+"-Posts.7z")
	}
	xmlFilename := "Posts.xml"

	xmlReader, err := getXmlReader(zipFilename, xmlFilename)
	if err != nil {
		return err
	}
	postParser, err := NewParser(xmlReader)
	if err != nil {
		return err
	}
	lookup := make([]uint32, 100*1000*1000)
	for postParser.Peek() != nil {
		post := postParser.Next()
		if post.PostTypeID == "2" {
			lookup[*post.ID] = uint32(*post.ParentID)
		} else {
			lookup[*post.ID] = uint32(*post.ID)
		}
	}
	fmt.Println(lookup[100])
	return nil
}

func SqliteIsBuilt(rootDir string, domain string) (bool, error) {
	ok, err := flagIsSet(rootDir, domain, "sqlite-built")
	return ok, err
}

func BuildSqlite(ctx context.Context, rootDir string, domain string) error {
	return fmt.Errorf("not implemented")
}

func getXmlReader(zipFilename string, xmlFilename string) (io.ReadCloser, error) {
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
