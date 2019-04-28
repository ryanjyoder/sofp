package sofp

import (
	"encoding/xml"
	"os"
	"time"
)

type RowsParser struct {
	postChan chan *Row
}

type Row struct {
	// Post attributes
	ID                    string `xml:"Id,attr" json:"Id"`
	PostTypeID            string `xml:"PostTypeId,attr" json:"PostTypeId,omitempty"`
	ParentID              string `xml:"ParentId,attr" json:"ParentId,omitempty:`
	AcceptedAnswerID      string `xml:"AcceptedAnswerId,attr" json:"AcceptedAnswerId,omitempty"`
	CreationDate          string `xml:"CreationDate,attr" json:"CreationDate,omitempty"`
	Score                 string `xml:"Score,attr" json:"Score,omitempty"`
	ViewCount             string `xml:"ViewCount,attr" json:"ViewCount,omitempty"`
	Body                  string `xml:"Body,attr" json:"Body,omitempty"`
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr" json:"CommunityOwnedDate,omitempty"`
	FavoriteCount         *int   `xml:"FavoriteCount,attr" json:"FavoriteCount,omitempty"`
	CommentCount          *int   `xml:"CommentCount,attr" json:"CommentCount,omitempty"`
	AnswerCount           *int   `xml:"AnswerCount,attr" json:"AnswerCount,omitempty"`
	Tags                  string `xml:"Tags,attr" json:"Tags,omitempty"`
	Title                 string `xml:"Title,attr" json:"Title,omitempty"`
	LastActivityDate      string `xml:"LastActivityDate,attr" json:"LastActivityDate,omitempty"`
	LastEditDate          string `xml:"LastEditDate,attr" json:"LastEditDate,omitempty"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" LastEditorDisplayName,omitempty"`
	LastEditorUserId      string `xml:"LastEditorUserId,attr" json:"LastEditorUserId,omitempty"`
	OwnerUserId           string `xml:"OwnerUserId,attr" json:"OwnerUserId,omitempty"`

	// PostHistory Attributes
	PostHistoryTypeID string `xml:"PostHistoryTypeId,attr" json:"PostHistoryTypeId,omitempty"`
	PostID            string `xml:"PostId,attr" json:"PostId,omitempty"`
	RevisionGUID      string `xml:"RevisionGUID,attr" json:"RevisionGUID,omitempty"`
	UserID            string `xml:"UserId,attr" json:"UserId,omitempty"`
	Comment           string `xml:"Comment,attr" json:"Comment,omitempty"`
	Text              string `xml:"Text,attr" json:"Text,omitempty"`
	UserDisplayName   string `xml:"UserDisplayName,attr" json:"UserDisplayName,omitempty"`

	// No Comments only attributes

	// PostLinks only attributes
	RelatedPostID string `xml:"RelatedPostId,attr" son:"RelatedPostId,omitempty:`
	LinkTypeID    string `xml:"LinkTypeId,attr" json:"LinkTypeId"`

	// Votes
	VoteTypeId int `xml:"VoteTypeId" json:"VoteTypeId,omitempty"`

	Stream    string `json:"StreamID"`
	DeltaType string `json:"DeltaType"`
	err       error
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
					if updateType == VotesType {
						p.CreationDate = timeMustParse(time.Parse("2006-01-02T15:04:05.999", p.CreationDate)).AddDate(0, 0, 1).Format("2006-01-02T15:04:05.999")
					}
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
