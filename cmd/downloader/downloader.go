package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {
	host := flag.String("H", "https://archive.org", "archive.org host address")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("storage directory must be provided")
		os.Exit(1)
	}
	storageDir := flag.Arg(0)

	d, err := sofp.NewDownloader(storageDir, *host)
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
