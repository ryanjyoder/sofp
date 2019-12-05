package sofp

import (
	"encoding/xml"
	"io"
	"time"
)

type RowsParser struct {
	postChan chan *Row
	peek     *Row
}

func NewParser(xmlFile io.ReadCloser) (*RowsParser, error) {
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
					psr.postChan <- &p
				}

			}
		}
	}()
	psr.Next() // This will initialize the parse and fill the peek variable
	return psr, nil
}
func timeMustParse(t time.Time, err error) time.Time {
	return t
}

func (psr *RowsParser) Next() *Row {
	p := psr.peek // the very first call peek will be nil. thus the parser must be initialized in the NewParse method
	psr.peek = <-psr.postChan
	return p

}

func (psr *RowsParser) Peek() *Row {
	return psr.peek
}

func (psr *RowsParser) Close() error {
	for _ = range psr.postChan {
	}
	psr.peek = nil
	return nil
}
