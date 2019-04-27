package main

import (
	"encoding/json"
	"fmt"
	"github.com/ryanjyoder/sofp"
	"os"
)

func main() {
	archive, err := sofp.NewArchiveParser(os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5])
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
