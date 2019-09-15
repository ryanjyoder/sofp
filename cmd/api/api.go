package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/ryanjyoder/sofp"
)

type Server struct {
	storageDir string
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Storage directory must be defined on the command line")
	}

	s := &Server{
		storageDir: os.Args[1],
	}

	r := gin.Default()
	v1 := r.Group("/v1")

	v1.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	v1.GET("/sites", s.ListSites)
	v1.GET("/sites/:domain/:version/"+sofp.FilenameSqlite+".gz", s.Sqlite)

	r.Run(":7770")
}

func (s *Server) ListSites(c *gin.Context) {
	sites := map[string]int{}
	err := filepath.Walk(s.storageDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			strippedPath := strings.TrimPrefix(path, s.storageDir)
			parts := strings.Split(strippedPath, "/")
			if parts[0] == "" {
				parts = parts[1:]
			}

			if len(parts) != 3 {
				return nil
			}
			fmt.Println(parts)
			if parts[2] != sofp.FilenameSqlite+".gz" {
				fmt.Println(parts[2], sofp.FilenameSqlite)
				return nil
			}
			domain := parts[0]
			version := parts[1]
			versionInt, _ := strconv.Atoi(version)
			sites[domain] = max(versionInt, sites[domain])
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	c.JSON(200, sites)
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Server) Sqlite(c *gin.Context) {
	domain := c.Param("domain")
	version := c.Param("version")
	sqlitePath := filepath.Join(s.storageDir, domain, version, sofp.FilenameSqlite+".gz")
	fmt.Println("looking for:", sqlitePath)
	c.File(sqlitePath)
}
