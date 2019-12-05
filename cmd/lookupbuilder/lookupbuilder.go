package main

import (
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {
	domains, err := sofp.ListDownloadedDomains(os.Args[1])
	fmt.Println(domains, err)
}
