package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"net/http"
)

func main() {
	storageDir := flag.String("s", ".", "Storage directory to serve files from")
	flag.Parse()
	mid := &EtagMiddleware{
		Handler: http.FileServer(http.Dir(*storageDir)),
	}
	log.Fatal(http.ListenAndServe(":8081", mid))
}

type EtagMiddleware struct {
	Handler http.Handler
}

func (m *EtagMiddleware) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	data := []byte(req.URL.String())
	etag := fmt.Sprintf(`"%x"`, md5.Sum(data))
	w.Header().Add("Etag", etag)

	m.Handler.ServeHTTP(w, req)
}
