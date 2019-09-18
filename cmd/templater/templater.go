package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/ryanjyoder/sofp"
)

var pageTemplate *template.Template
var StreamsDBs map[string]*sql.DB

func main() {
	//resp, err := http.Get("http://so.gearfar.com:7770/v1/sites")
	resp, err := http.Get("http://127.0.0.1:7770/v1/sites")
	if err != nil {
		fmt.Println("error loading available sites:", err)
		os.Exit(1)
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		fmt.Println("error reading response:", err)
		os.Exit(1)
	}

	sites := map[string]int{}
	err = json.Unmarshal(body, &sites)
	if err != nil {
		fmt.Println("error loading sites:", err)
		os.Exit(1)
	}
	storageDir := os.Args[1]
	dbs, err := getDbs(storageDir, sites)

	fmt.Println(dbs, err)

	StreamsDBs = dbs

	pageTemplateFilepath := filepath.Join(os.Args[2], "question.tpl")
	staticAssetsDir := filepath.Join(os.Args[2], "static")
	pageTemplate, err = template.ParseFiles(pageTemplateFilepath)
	if err != nil {
		log.Fatal("failed to load tempalte:", err)
	}
	http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(staticAssetsDir))))
	http.HandleFunc("/", viewHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getDbs(storageDir string, sites map[string]int) (map[string]*sql.DB, error) {
	dbs := map[string]*sql.DB{}
	for domain, version := range sites {
		dbFilepath := fmt.Sprintf("%s/%s/%d/streams.sqlite", storageDir, domain, version)
		database, err := sql.Open("sqlite3", dbFilepath)
		if err != nil {
			return nil, err
		}
		dbs[domain] = database
	}
	return dbs, nil
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	fmt.Println("handing page:", r.URL.Path)
	switch len(parts) {
	case 2:
		if parts[1] == "" {
			rootHandler(w, r)
			break
		}
		indexHandler(w, r)
	case 4:
		questionHandler(w, r)
	default:
		http.Error(w, "incorrect format", 404)
	}

}
func rootHandler(w http.ResponseWriter, r *http.Request) {
	for domain := range StreamsDBs {
		fmt.Fprintf(w, "<a href='/%s'>%s</a></br>\n", domain, domain)
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 2 {
		http.Error(w, "incorrect format", 404)
		return
	}
	domain := parts[1]
	db, ok := StreamsDBs[domain]
	if !ok {
		http.Error(w, "domain not found", 404)
		return
	}

	rows, err := db.Query(`select distinct streamID from deltas`)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	for rows.Next() {
		streamID := ""

		err := rows.Scan(&streamID)
		if err != nil {

			http.Error(w, err.Error(), 500)
			return
		}
		idParts := strings.Split(streamID, "/")
		href := idParts[0] + "/questions/" + idParts[1]

		fmt.Fprintf(w, "<a href='/%s'>%s</a></br>\n", href, streamID)
	}

}

func questionHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	fmt.Println("handing page:", r.URL.Path)
	if len(parts) != 4 {
		http.Error(w, "incorrect format", 404)
		return
	}
	domain := parts[1]
	id := parts[3]
	p, err := loadPage(domain, id)
	if err != nil {
		log.Println("error loading page data:", err)
		http.Error(w, err.Error(), 404)
		return
	}

	pageTemplate.Execute(w, p)
}

func loadPage(domain, id string) (*sofp.Question, error) {
	fmt.Println("loading page:", domain, id)
	db, ok := StreamsDBs[domain]
	if !ok {
		return nil, fmt.Errorf("domain not found")
	}

	rows, err := db.Query(`
	SELECT msg FROM deltas WHERE streamID = ? order by ordering
	`, domain+"/"+id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deltas := []*sofp.Row{}
	for rows.Next() {
		msg := ""
		delta := &sofp.Row{}
		err := rows.Scan(&msg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(msg), delta)
		if err != nil {
			return nil, err
		}
		deltas = append(deltas, delta)
	}

	if len(deltas) < 1 {
		return nil, fmt.Errorf("page not found")
	}

	question := &sofp.Question{}

	for i := range deltas {
		question.AppendRow(deltas[i])
	}

	return question, nil
}
