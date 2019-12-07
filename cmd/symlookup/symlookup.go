package main

import (
	"fmt"
	"os"
)

func main() {
	realname, err := os.Readlink(os.Args[1])
	fmt.Println("real name:", realname, err)
}
