package main

import (
	"fmt"
	"github.com/cavaliercoder/grab"
	"github.com/ryanjyoder/sofp"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Println("please provide a working directory and a stackoverflow domain to sync")
	}

	workingDir := os.Args[1]
	archiveDir := filepath.Join(workingDir, "1-zips")
	xmlDir := filepath.Join(workingDir, "2-xmls")
	parsedDir := filepath.Join(workingDir, "3-streams")

	for _, domain := range []string{os.Args[2]} {
		filename := domain + ".7z"
		outputfile := filepath.Join(archiveDir, filename)
		archiveURL := "https://archive.org/download/stackexchange/" + filename
		fmt.Println("downloading", outputfile, archiveURL)
		_, err := grab.Get(outputfile, archiveURL)
		if err != nil {
			checkerr("error downloading archive", err)
		}

		decompressedFiles := filepath.Join(xmlDir, domain)
		err = os.MkdirAll(decompressedFiles, 0755)
		checkerr("couldn't created xml directory", err)

		cmd := exec.Command("7z", "x", outputfile)
		cmd.Dir = decompressedFiles

		fmt.Println("Decompressing files:", outputfile)
		err = cmd.Run()
		checkerr("error decomporessing archive", err)

		archive, err := sofp.NewArchiveParser(sofp.GetFilepathsFromDir(decompressedFiles))
		checkerr("error open archive", err)

		streamDir := filepath.Join(parsedDir, domain)
		writer, err := sofp.NewStreamWriter(streamDir)
		checkerr("could not create stream writer", err)

		fmt.Println("Parsing to streams", decompressedFiles, streamDir)
		for post := archive.Next(); post != nil; post = archive.Next() {
			writer.Write(post)
		}
	}
}

func checkerr(msg string, err error) {
	if err != nil {
		fmt.Println(msg + ": " + err.Error())
		os.Exit(1)
	}
}
