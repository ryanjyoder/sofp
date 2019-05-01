package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/ryanjyoder/sofp"
	"log"
	"os"
)

func main() {
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

	jsonstr, _ := json.Marshal(question)
	fmt.Println(string(jsonstr))
}
