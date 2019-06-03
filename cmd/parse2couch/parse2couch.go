package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cavaliercoder/grab"
	"github.com/ryanjyoder/couchdb"
	"github.com/ryanjyoder/sofp"
)

const (
	PostsDeltaType       = "Posts"
	PostHistoryDeltaType = "PostHistory"
)

func main() {
	workingDir, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	domain := os.Args[2]

	u, err := url.Parse("http://127.0.0.1:5984/")
	if err != nil {
		log.Fatal(err)
	}
	// create a new client
	client, err := couchdb.NewAuthClient("admin", "admin", u)
	if err != nil {
		log.Fatal(err)
	}
	dbName := strings.ReplaceAll(domain, ".", "_")
	resp, err := client.Create(dbName)
	if err != nil {
		cErr, ok := err.(*couchdb.Error)
		if !(ok && cErr.StatusCode == 412) {
			log.Fatal(resp, err)
		}
	}
	db := client.Use(dbName)

	domainDir := filepath.Join(workingDir, domain)
	os.MkdirAll(domainDir, 0755)

	archiveDir := filepath.Join(domainDir, "zips")
	filenames := get7zFilenames(domain)

	for _, filename := range filenames {
		outputfile := filepath.Join(archiveDir, filename)
		archiveURL := "https://archive.org/download/stackexchange/" + filename
		fmt.Println("downloading", outputfile, archiveURL)
		_, err := grab.Get(outputfile, archiveURL)
		if err != nil {
			log.Fatal(err)
		}
	}

	postsZip := get7zFilename(domain, sofp.PostsType)
	zipFile := filepath.Join(archiveDir, postsZip)

	cmd := exec.Command("7z", "e", "-so", zipFile, sofp.PostsType+".xml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	psr, err := sofp.NewParser(stdout, sofp.PostsType)
	if err != nil {
		log.Fatal("couldnt open parse:", err)
	}
	cmd.Start()
	idToParent := map[int]int{}
	for row := psr.Next(); row != nil; row = psr.Next() {
		idToParent[getInt(row.ID)] = getInt(row.ID)
		if getInt(row.ParentID) != 0 {
			idToParent[getInt(row.ID)] = getInt(row.ParentID)
		}
	}

	// PostHistory
	postHistoryZip := get7zFilename(domain, sofp.PostHistoryType)
	zipFile = filepath.Join(archiveDir, postHistoryZip)

	cmd = exec.Command("7z", "e", "-so", zipFile, sofp.PostHistoryType+".xml")
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	psr, err = sofp.NewParser(stdout, sofp.PostHistoryType)
	if err != nil {
		log.Fatal("couldnt open parse:", err)
	}
	cmd.Start()

	for row := psr.Next(); row != nil; row = psr.Next() {
		row.Stream = fmt.Sprintf("%d", idToParent[getInt(row.PostID)])
		_, err := db.Post(row)
		if err != nil {
			cErr, ok := err.(*couchdb.Error)
			if !(ok && cErr.StatusCode == 409) {
				log.Fatal(resp, err)
			}
		}
	}

	// Comments
	commentsZip := get7zFilename(domain, sofp.CommentsType)
	zipFile = filepath.Join(archiveDir, commentsZip)

	cmd = exec.Command("7z", "e", "-so", zipFile, sofp.CommentsType+".xml")
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	psr, err = sofp.NewParser(stdout, sofp.CommentsType)
	if err != nil {
		log.Fatal("couldnt open parse:", err)
	}
	cmd.Start()

	for row := psr.Next(); row != nil; row = psr.Next() {
		row.Stream = fmt.Sprintf("%d", idToParent[getInt(row.PostID)])
		_, err := db.Post(row)
		if err != nil {
			cErr, ok := err.(*couchdb.Error)
			if !(ok && cErr.StatusCode == 409) {
				log.Fatal(resp, err)
			}
		}
	}

	// Votes
	votesZip := get7zFilename(domain, sofp.VotesType)
	zipFile = filepath.Join(archiveDir, votesZip)

	cmd = exec.Command("7z", "e", "-so", zipFile, sofp.VotesType+".xml")
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	psr, err = sofp.NewParser(stdout, sofp.VotesType)
	if err != nil {
		log.Fatal("couldnt open parse:", err)
	}
	cmd.Start()

	for row := psr.Next(); row != nil; row = psr.Next() {
		row.Stream = fmt.Sprintf("%d", idToParent[getInt(row.PostID)])
		_, err := db.Post(row)
		if err != nil {
			cErr, ok := err.(*couchdb.Error)
			if !(ok && cErr.StatusCode == 409) {
				log.Fatal(resp, err)
			}
		}
	}

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

func get7zFilename(domain string, deltaType string) string {
	if domain == "stackoverflow.com" {
		return domain + "-" + deltaType + ".7z"
	}
	return domain + ".7z"
}

func getInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}
