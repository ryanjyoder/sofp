package sofp

import (
	"encoding/xml"
	"io"
	"time"
)

type RowsParser struct {
	postChan chan *Row
}

func (row *Row) StreamID() string {
	return row.Stream
}

func NewParser(xmlFile io.Reader) (*RowsParser, error) {
	psr := &RowsParser{
		postChan: make(chan *Row),
	}

	go func() {
		defer close(psr.postChan)
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
