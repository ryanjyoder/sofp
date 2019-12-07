package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {
	rootDir := os.Args[1]
	domains, err := sofp.ListDownloadedDomains(rootDir)
	fmt.Println(domains, err)

	for _, domain := range domains {
		built, err := sofp.PostIDLookupIsBuilt(rootDir, domain)
		if built {
			fmt.Println("lookup db already built:", domain)
			continue
		}
		if err != nil {
			fmt.Println("error checking lookup db:", err)
			continue
		}
		version, err := sofp.BuildPostIDLookup(context.TODO(), rootDir, domain)
		if err != nil {
			fmt.Println("Error building lookup:", err)
		}
		sofp.SetLookupBuilt(rootDir, domain, version)
	}

}
