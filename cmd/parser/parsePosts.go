package main

import (
	"encoding/json"
	"fmt"
	"github.com/ryanjyoder/sofp"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("please provide a directory with the archive")
	}
	archive, err := sofp.NewArchiveParser(sofp.GetFilepathsFromDir(os.Args[1]))
	checkerr("error open archive", err)

	for post := archive.Next(); post != nil; post = archive.Next() {
		jsonstr, _ := json.Marshal(post)
		fmt.Println(string(jsonstr))
	}
}

func checkerr(msg string, err error) {
	if err != nil {
		fmt.Println(msg + ": " + err.Error())
		os.Exit(1)
	}
}
