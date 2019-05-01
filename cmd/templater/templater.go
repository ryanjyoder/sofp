package main

import (
	"bufio"
	"encoding/json"
	"github.com/ryanjyoder/sofp"
	"html/template"
	"log"
	"net/http"
	"os"
)

var pageTemplate *template.Template

func main() {
	var err error
	pageTemplate, err = template.ParseFiles("template/stackoverflow.html")
	if err != nil {
		log.Fatal("failed to load tempalte:", err)
	}
	http.Handle("/page/assets/", http.StripPrefix("/page/assets/", http.FileServer(http.Dir(os.Args[2]))))
	http.HandleFunc("/page/", viewHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	p, err := loadPage("title")
	if err != nil {
		log.Fatal("error loading page data:", err)
	}

	pageTemplate.Execute(w, p)
}

func loadPage(id string) (*sofp.Question, error) {
	filename := os.Args[1]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
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