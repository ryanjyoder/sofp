package main

import (
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Println("please provide a working directory and a stackoverflow domain to sync")
		os.Exit(1)
	}
	worker, err := sofp.NewWorker(os.Args[1])
	checkerr("failed to create worker", err)

	err = worker.Run()
	checkerr("couldnt finish parsing:", err)
}

func checkerr(msg string, err error) {
	if err != nil {
		fmt.Println(msg + ": " + err.Error())
		os.Exit(1)
	}
}
