package sofp

import (
	"encoding/xml"
	"os"
	"time"
)

type RowsParser struct {
	postChan chan *Row
}

func (row *Row) StreamID() string {
	return row.Stream
}

func NewParser(file string, updateType string) (*RowsParser, error) {

	xmlFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	psr := &RowsParser{
		postChan: make(chan *Row),
	}

	go func() {
		defer close(psr.postChan)
		defer xmlFile.Close()
		decoder := xml.NewDecoder(xmlFile)

		for {
			// Read tokens from the XML document in a stream.
			t, _ := decoder.Token()
			if t == nil {
				break
			}

			// Inspect the type of the token just read.
			if se, ok := t.(xml.StartElement); ok {
				if se.Name.Local == "row" {
					var p Row
					err := decoder.DecodeElement(&p, &se)
					p.err = err
					p.DeltaType = updateType
					psr.postChan <- &p
				}

			}
		}
	}()
	return psr, nil
}
func timeMustParse(t time.Time, err error) time.Time {
	return t
}

func (psr *RowsParser) Next() *Row {

	return <-psr.postChan
}
