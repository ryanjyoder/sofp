package sofp

import (
	"fmt"
	"log"
	"strings"

	"github.com/ryanjyoder/couchdb"
)

func (w *Worker) prepareDB(domain string) (couchdb.DatabaseService, error) {
	dbName := strings.ReplaceAll(domain, ".", "_")
	_, err := w.couchClient.Create(dbName)
	if err != nil {
		cErr, ok := err.(*couchdb.Error)
		if !(ok && cErr.StatusCode == 412) {
			return nil, err
		}
	}
	db := w.couchClient.Use(dbName)

	return db, loadViews(db)

}

type VersionedDesignDoc struct {
	couchdb.DesignDocument
	Version int `json:"version"`
}

func (d *VersionedDesignDoc) GetID() string {
	return d.GetID()
}

func (d *VersionedDesignDoc) GetRev() string {
	return d.GetRev()
}

func loadViews(db couchdb.DatabaseService) error {
	checkpointView := VersionedDesignDoc{
		DesignDocument: couchdb.DesignDocument{
			Document: couchdb.Document{
				ID: "_design/checkpoint",
			},
			Views: map[string]couchdb.DesignDocumentView{
				"id-stats": couchdb.DesignDocumentView{
					Map:    "function (doc) {\n  emit(doc.DeltaType, doc.Id);\n}",
					Reduce: "_stats",
				},
			},
			Language: "javascript",
		},
		Version: 3,
	}

	currentCheckpointView := VersionedDesignDoc{}
	db.Get(&currentCheckpointView, checkpointView.ID)
	// do nothing with an error. version will default to zero

	// if View update to date, nothing to do
	if currentCheckpointView.Version >= checkpointView.Version {
		log.Println("view is up-to-date with current version")
		return nil
	}
	log.Println("updating view from version:", currentCheckpointView.Version, "->", checkpointView.Version)

	checkpointView.Rev = currentCheckpointView.Rev
	_, err := db.Post(&checkpointView)
	if err != nil {
		return err
	}
	return nil
}

func getLastSeenID(db couchdb.DatabaseService, deltaType string) (int, error) {
	//keys=["PostHistory"]&descending=true&reduce=true&group=true
	keyQuery := fmt.Sprintf("\"%s\"", deltaType)
	queryParams := couchdb.QueryParameters{
		Reduce: boolRef(true),
		Group:  boolRef(true),
		Key:    stringRef(keyQuery),
	}
	timedOut := true
	var resp *couchdb.ViewResponse
	var err error
	for timedOut {
		timedOut = false
		resp, err = db.View("checkpoint").Get("id-stats", queryParams)
		if cErr, ok := err.(*couchdb.Error); ok {
			timedOut = cErr.Type == "timeout"
			if timedOut {
				log.Println("view timeout. trying again")
			}
		}

	}

	if err != nil || len(resp.Rows) < 1 {
		return 0, err
	}

	valueI, ok := resp.Rows[0].Value.(map[string]interface{})
	if !ok {
		return 0, nil
	}
	maxI, ok := valueI["max"]
	if !ok {
		return 0, nil
	}
	max, ok := maxI.(float64)
	if !ok {
		return 0, nil
	}

	return int(max), nil
}

func boolRef(b bool) *bool {
	return &b
}
func stringRef(s string) *string {
	return &s
}

/*
{
	"_id": "_design/checkpoints",
	"_rev": "2-60a434ecc52b8ab9efe3fb40138917a6",
	"views": {
	  "id-stats": {
		"reduce": "_stats",
		"map": "function (doc) {\n  emit(doc.DeltaType, doc.Id);\n}"
	  }
	},
	"language": "javascript"
  }*/
