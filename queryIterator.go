package sofp

import (
	"log"

	"github.com/ryanjyoder/couchdb"
)

type QueryIterator struct {
	rowChan chan *couchdb.Row
	errChan chan error
}

func NewQueryIterator(db couchdb.DatabaseService, designDoc string, viewName string) (*QueryIterator, error) {
	pageSize := 500
	skip := 0

	rowChan := make(chan *couchdb.Row)
	errChan := make(chan error)

	go func() {
		defer close(rowChan)
		defer close(errChan)
		for {
			view, err := db.View(designDoc).Get(viewName, couchdb.QueryParameters{
				Limit:  intRef(pageSize),
				Skip:   intRef(skip),
				Reduce: boolRef(false),
			})
			if cErr, ok := err.(*couchdb.Error); ok && cErr.Type == "timeout" {
				continue
			} else if err != nil {
				rowChan <- nil
				errChan <- err
				return
			}
			if len(view.Rows) < 1 {
				return
			}
			for _, r := range view.Rows {
				rowChan <- &r
				errChan <- nil
			}
			skip += pageSize
		}

	}()

	itr := QueryIterator{
		rowChan: rowChan,
		errChan: errChan,
	}

	return &itr, nil

}

func (itr *QueryIterator) Next() (*couchdb.Row, error) {
	if itr == nil {
		log.Fatal("itr is nillllllll:", itr)
	}
	r := <-itr.rowChan
	err := <-itr.errChan
	return r, err
}
