package sofp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cavaliercoder/grab"
	"golang.org/x/sync/semaphore"

	_ "github.com/mattn/go-sqlite3"
)

const (
	ZipSubir      = "1-zips"
	XmlSubdir     = "2-xmls"
	SqliteSubdir  = "3-sqlite"
	StreamsSubdir = "4-streams"
)

type Worker struct {
	workingDir          string
	db                  *sql.DB
	dbMutex             *sync.Mutex
	decompressSemaphore *semaphore.Weighted
}

func NewWorker(workingDir string) (*Worker, error) {

	workingDir, err := filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}
	os.MkdirAll(workingDir, 0755)
	dbFilepath := filepath.Join(workingDir, "state.sqlite")
	database, err := sql.Open("sqlite3", dbFilepath)
	if err != nil {
		return nil, err
	}

	statement, err := database.Prepare(`
		CREATE TABLE IF NOT EXISTS sites (
			domain text PRIMARY KEY,
			active BOOLEAN,
			archiveLastModified DATETIME,
			downloadComplete BOOLEAN,
			isDecompressed BOOLEAN,
			lastDeltaType TEXT,
			lastDeltaId INT
		)`)
	if err != nil {
		return nil, err
	}
	_, err = statement.Exec()
	if err != nil {
		return nil, err
	}

	return &Worker{
		workingDir:          workingDir,
		db:                  database,
		dbMutex:             &sync.Mutex{},
		decompressSemaphore: semaphore.NewWeighted(8),
	}, nil
}

func (w *Worker) Run() error {
	domains, err := w.getSetActiveDomains()
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	for _, domain := range domains {
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			w.processDomain(d)
		}(domain)
	}
	wg.Wait()
	return nil
}

func (w *Worker) processDomain(domain string) error {
	err := w.downloadZips(domain)
	if err != nil {
		return err
	}
	err = w.decompressZips(domain)
	if err != nil {
		return err
	}

	err = w.parseXml(domain)
	if err != nil {
		return err
	}
	return nil
}

func (w *Worker) parseXml(domain string) error {
	xmlDomainDir := filepath.Join(w.workingDir, ZipSubir, domain)

	archive, err := NewArchiveParser(GetFilepathsFromDir(xmlDomainDir))
	if err != nil {
		return err
	}

	sqliteDomainDir := filepath.Join(w.workingDir, SqliteSubdir, domain)
	writer, err := NewStreamWriter(sqliteDomainDir)
	if err != nil {
		return err
	}

	fmt.Println("Parsing to streams", xmlDomainDir, sqliteDomainDir)
	lastDelta := &Row{}
	checkpointType, checkpointID, err := w.getCheckpoint(domain)
	if err != nil {
		return err
	}
	streamIsReset := checkpointType == ""
	if !streamIsReset {
		fmt.Println("resetting stream:")
	}
	for delta := archive.Next(); delta != nil; delta = archive.Next() {
		lastDelta = delta
		if delta.DeltaType == checkpointType && toInt(delta.ID) == checkpointID {
			streamIsReset = true
			fmt.Println("\nstream reset")
			continue
		}
		if !streamIsReset {
			continue
		}

		err = writer.Write(delta)
		if err != nil {
			return err
		}
	}

	streamsDomainDir := filepath.Join(w.workingDir, StreamsSubdir, domain)
	err = writer.ExportStreams(streamsDomainDir)
	if err != nil {
		return nil
	}

	fmt.Println("parsing done ")
	err = w.setCheckpoint(domain, lastDelta.DeltaType, *lastDelta.ID)
	if err != nil {
		fmt.Println("Error saving checkpoint:", err)
	}
	return nil
}

func (w *Worker) downloadZips(domain string) error {
	archiveDir := filepath.Join(w.workingDir, ZipSubir)
	filenames := get7zFilenames(domain)

	for _, filename := range filenames {
		outputfile := filepath.Join(archiveDir, filename)
		archiveURL := "https://archive.org/download/stackexchange/" + filename
		fmt.Println("downloading", outputfile, archiveURL)
		_, err := grab.Get(outputfile, archiveURL)
		if err != nil {
			return err
		}
		err = w.downloadComplete(domain)
		if err != nil {
			fmt.Println("couldnt set download complete (continuing anyways:", err.Error())
		}

	}
	return nil
}

func (w *Worker) decompressZips(domain string) error {
	ok, err := w.isDecompressed(domain)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	w.decompressSemaphore.Acquire(context.Background(), 1)
	defer w.decompressSemaphore.Release(1)

	archiveDir := filepath.Join(w.workingDir, ZipSubir)
	xmlDir := filepath.Join(w.workingDir, XmlSubdir)

	filenames := get7zFilenames(domain)

	xmlDomainDir := filepath.Join(xmlDir, domain)

	err = os.MkdirAll(xmlDomainDir, 0755)
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		zipFile := filepath.Join(archiveDir, filename)

		cmd := exec.Command("7z", "x", "-y", zipFile)
		cmd.Dir = xmlDomainDir

		fmt.Println("Decompressing files:", zipFile)
		stdoutStderr, err := cmd.CombinedOutput()
		fmt.Println("7z message", string(stdoutStderr))
		if err != nil {
			return err
		}
	}
	w.setDecompressed(domain, true)
	return nil
}

func (w *Worker) getSetActiveDomains() ([]string, error) {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	domainsStr, _ := os.LookupEnv("SOFP_DOMAINS")
	if domainsStr == "" {
		return nil, fmt.Errorf("SOFP_DOMAINS env not set")
	}
	domains := strings.Split(domainsStr, ",")
	// reset all sites to inactive
	w.db.Exec("UPDATE sites SET active=false")

	//set only current domains to active
	for _, domain := range domains {
		_, err := w.db.Exec(`
			INSERT INTO sites
				(domain, active)
			VALUES 
				(?, true)
			ON CONFLICT (domain) DO UPDATE SET
				active=true`, domain)
		if err != nil {
			fmt.Println("error inserting:", err.Error())
			return nil, err
		}
	}

	return domains, nil
}

func (w *Worker) downloadComplete(domain string) error {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	_, err := w.db.Exec(`UPDATE sites SET downloadComplete=true WHERE domain=?`, domain)
	return err
}

func (w *Worker) setCheckpoint(domain string, deltaType string, deltaID int) error {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	fmt.Println("saving checkpoint:", domain, deltaType, deltaID)
	_, err := w.db.Exec("UPDATE sites SET lastDeltaType=?, lastDeltaId=? WHERE domain=?", deltaType, deltaID, domain)
	return err
}

func (w *Worker) getCheckpoint(domain string) (string, int, error) {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	rows, err := w.db.Query("Select lastDeltaType, lastDeltaId from sites where domain=?", domain)
	if err != nil {
		return "", 0, err
	}
	var checkpointType *string
	var checkpointID *int
	if rows.Next() {
		err = rows.Scan(&checkpointType, &checkpointID)
	}
	if checkpointType == nil {
		p := ""
		checkpointType = &p
	}
	if checkpointID == nil {
		i := 0
		checkpointID = &i
	}
	return *checkpointType, *checkpointID, err
}

func (w *Worker) isDecompressed(domain string) (bool, error) {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	rows, err := w.db.Query("Select isDecompressed from sites where domain=?", domain)
	if err != nil {
		return false, err
	}

	isDecompressed := false
	if rows.Next() {
		err = rows.Scan(&isDecompressed)
	}

	return isDecompressed, err
}

func (w *Worker) setDecompressed(domain string, complete bool) error {
	w.dbMutex.Lock()
	defer w.dbMutex.Unlock()

	_, err := w.db.Exec(`UPDATE sites SET isDecompressed=true WHERE domain=?`, domain)
	return err
}

func toInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func get7zFilenames(domain string) []string {
	if domain == "stackoverflow.com" {
		return []string{
			domain + "-Badges.7z",
			domain + "-Comments.7z",
			domain + "-PostHistory.7z",
			domain + "-PostLinks.7z",
			domain + "-Posts.7z",
			domain + "-Tags.7z",
			domain + "-Users.7z",
			domain + "-Votes.7z",
		}
	}

	return []string{domain + ".7z"}

}
