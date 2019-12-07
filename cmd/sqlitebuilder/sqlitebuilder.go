package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ryanjyoder/sofp"
)

func main() {
	rootDir := os.Args[1]
	domains, err := sofp.ListLookupBuiltDomains(rootDir)
	fmt.Println(domains, err)

	for _, domain := range domains {
		built, err := sofp.SqliteIsBuilt(rootDir, domain)
		if built {
			fmt.Println("sqlite already built:", domain)
			continue
		}
		if err != nil {
			fmt.Println("error checking lookup db:", err)
			continue
		}
		fmt.Println("Building sqlite for:", domain)
		version, err := sofp.BuildSqlite(context.TODO(), rootDir, domain)
		if err != nil {
			fmt.Println("Error building lookup:", err)
		}
		sofp.SetSqliteBuilt(rootDir, domain, version)
	}

}
