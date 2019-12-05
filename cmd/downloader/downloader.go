package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {

	d, err := sofp.NewDownloader(os.Args[1], "https://archive.org")
	if err != nil {
		fmt.Println("error creating downloader:", err)
		os.Exit(1)
	}

	err = d.Run(context.TODO())
	if err != nil {
		fmt.Println("error downloading:", err)
		os.Exit(1)
	}
}
