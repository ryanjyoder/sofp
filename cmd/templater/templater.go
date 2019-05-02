package main

import (
	"bufio"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ryanjyoder/sofp"
)

var pageTemplate *template.Template

func main() {
	var err error
	pageTemplateFilepath := filepath.Join(os.Args[2], "stackoverflow.html")
	staticAssetsDir := filepath.Join(os.Args[2], "static")
	pageTemplate, err = template.ParseFiles(pageTemplateFilepath)
	if err != nil {
		log.Fatal("failed to load tempalte:", err)
	}
	http.Handle("/page/assets/", http.StripPrefix("/page/assets/", http.FileServer(http.Dir(staticAssetsDir))))
	http.HandleFunc("/page/", viewHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	p, err := loadPage(filepath.Base(r.URL.Path))
	if err != nil {
		log.Println("error loading page data:", err)
		http.Error(w, err.Error(), 404)
		return
	}

	pageTemplate.Execute(w, p)
}

func loadPage(id string) (*sofp.Question, error) {
	streamsDir := os.Args[1]
	paddedFilename := id + "000"
	filepath := filepath.Join(streamsDir, paddedFilename[:3], id)

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if ok := scanner.Scan(); !ok {
		log.Fatal("error getting question from stream:", scanner.Err())
	}

	questionStr := scanner.Text()
	row := sofp.Row{}
	json.Unmarshal([]byte(questionStr), &row)
	question, err := row.GetQuestion()
	if err != nil {
		log.Fatal("first row is not a question:", err)
	}

	for scanner.Scan() {
		rowStr := scanner.Text()
		row := sofp.Row{}
		err = json.Unmarshal([]byte(rowStr), &row)
		if err != nil {
			log.Fatal("Error reading stream:", err, rowStr)
		}
		question.AppendRow(&row)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return question, nil
}
