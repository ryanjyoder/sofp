package main

import (
	"fmt"
	"github.com/ryanjyoder/sofp"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("please provide a directory with the archive and output directory")
	}
	archive, err := sofp.NewArchiveParser(sofp.GetFilepathsFromDir(os.Args[1]))
	checkerr("error open archive", err)

	writer, err := sofp.NewStreamWriter(os.Args[2])
	checkerr("could not create stream writer", err)

	for post := archive.Next(); post != nil; post = archive.Next() {
		writer.Write(post)
	}
}

func checkerr(msg string, err error) {
	if err != nil {
		fmt.Println(msg + ": " + err.Error())
		os.Exit(1)
	}
}
