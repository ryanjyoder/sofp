package sofp

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cavaliercoder/grab"

	_ "github.com/mattn/go-sqlite3"
)

type Worker struct {
	workingDir string
	db         *sql.DB
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
			decompressed BOOLEAN,
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
		workingDir: workingDir,
		db:         database,
	}, nil
}

func (w *Worker) Run() error {
	domains, err := w.getSetActiveDomains()
	if err != nil {
		return err
	}

	archiveDir := filepath.Join(w.workingDir, "1-zips")
	xmlDir := filepath.Join(w.workingDir, "2-xmls")
	parsedDir := filepath.Join(w.workingDir, "3-streams")

	for _, domain := range domains {
		filenames := []string{domain + ".7z"}
		if domain == "stackoverflow.com" {
			filenames = []string{
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
		decompressedFiles := filepath.Join(xmlDir, domain)

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

			err = os.MkdirAll(decompressedFiles, 0755)
			if err != nil {
				return err
			}

			cmd := exec.Command("7z", "x", "-y", outputfile)
			cmd.Dir = decompressedFiles

			fmt.Println("Decompressing files:", outputfile)
			stdoutStderr, err := cmd.CombinedOutput()
			fmt.Println("7z message", string(stdoutStderr))
			if err != nil {
				return nil
			}
		}

		archive, err := NewArchiveParser(GetFilepathsFromDir(decompressedFiles))
		if err != nil {
			return err
		}

		streamDir := filepath.Join(parsedDir, domain)
		writer, err := NewStreamWriter(streamDir)
		if err != nil {
			return err
		}
		defer writer.Shutdown()

		fmt.Println("Parsing to streams", decompressedFiles, streamDir)
		lastDelta := &Row{}
		checkpointType, checkpointID, err := w.getCheckpoint(domain)
		if err != nil {
			return err
		}
		streamIsReset := checkpointType == ""
		fmt.Println("resetting stream:")
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
		fmt.Println("parsing done ")
		err = w.setCheckpoint(domain, lastDelta.DeltaType, *lastDelta.ID)
		if err != nil {
			fmt.Println("Error saving checkpoint:", err)
		}
	}
	return nil
}

func (w *Worker) getSetActiveDomains() ([]string, error) {
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
	_, err := w.db.Exec(`UPDATE sites SET downloadComplete=true WHERE domain=?`, domain)
	return err
}

func (w *Worker) setCheckpoint(domain string, deltaType string, deltaID int) error {
	fmt.Println("saving checkpoint:", domain, deltaType, deltaID)
	_, err := w.db.Exec("UPDATE sites SET lastDeltaType=?, lastDeltaId=? WHERE domain=?", deltaType, deltaID, domain)
	return err
}

func (w *Worker) getCheckpoint(domain string) (string, int, error) {
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

func toInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}
