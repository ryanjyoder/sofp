package main

import (
	"log"
	"os"

	sofp "github.com/ryanjyoder/sofp"
)

func main() {

	if len(os.Args) < 2 {
		log.Fatal("must provide a storage directory")
	}
	cfg, err := sofp.GetDefaultConfigs()
	if err != nil {
		log.Fatal("couldn't get configs:", err)
	}
	worker, err := sofp.NewWorker(cfg)
	if err != nil {
		log.Fatal("couldn't create new worker:", err)
	}
	err = worker.Run()
	if err != nil {
		log.Fatal("worker failed:", err)
	}
}
