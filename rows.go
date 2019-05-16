package sofp

import (
	"fmt"
	"html/template"
)

type Row struct {
	// Post attributes
	ID                    *int   `xml:"Id,attr" json:"Id"`
	PostTypeID            string `xml:"PostTypeId,attr" json:"PostTypeId,omitempty"`
	ParentID              *int   `xml:"ParentId,attr" json:"ParentId,omitempty"`
	AcceptedAnswerID      *int   `xml:"AcceptedAnswerId,attr" json:"AcceptedAnswerId,omitempty"`
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
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" json:"LastEditorDisplayName,omitempty"`
	LastEditorUserId      string `xml:"LastEditorUserId,attr" json:"LastEditorUserId,omitempty"`
	OwnerUserId           string `xml:"OwnerUserId,attr" json:"OwnerUserId,omitempty"`

	// PostHistory Attributes
	PostHistoryTypeID string `xml:"PostHistoryTypeId,attr" json:"PostHistoryTypeId,omitempty"`
	PostID            *int   `xml:"PostId,attr" json:"PostId,omitempty"`
	RevisionGUID      string `xml:"RevisionGUID,attr" json:"RevisionGUID,omitempty"`
	UserID            string `xml:"UserId,attr" json:"UserId,omitempty"`
	Comment           string `xml:"Comment,attr" json:"Comment,omitempty"`
	Text              string `xml:"Text,attr" json:"Text,omitempty"`
	UserDisplayName   string `xml:"UserDisplayName,attr" json:"UserDisplayName,omitempty"`

	// No Comments only attributes

	// PostLinks only attributes
	RelatedPostID string `xml:"RelatedPostId,attr" json:"RelatedPostId,omitempty"`
	LinkTypeID    string `xml:"LinkTypeId,attr" json:"LinkTypeId,omitempty"`

	// Votes
	VoteTypeId int `xml:"VoteTypeId,attr" json:"VoteTypeId,omitempty"`

	Stream    string `json:"StreamID"`
	DeltaType string `json:"DeltaType"`
	err       error
}

type Question struct {
	// Post attributes
	ID                    int           `xml:"Id,attr" json:"Id"`
	AcceptedAnswerID      int           `xml:"AcceptedAnswerId,attr" json:"AcceptedAnswerId,omitempty"`
	CreationDate          string        `xml:"CreationDate,attr" json:"CreationDate,omitempty"`
	Score                 string        `xml:"Score,attr" json:"Score,omitempty"`
	ViewCount             string        `xml:"ViewCount,attr" json:"ViewCount,omitempty"`
	Body                  template.HTML `xml:"Body,attr" json:"Body,omitempty"`
	CommunityOwnedDate    string        `xml:"CommunityOwnedDate,attr" json:"CommunityOwnedDate,omitempty"`
	FavoriteCount         int           `xml:"FavoriteCount,attr" json:"FavoriteCount,omitempty"`
	CommentCount          int           `xml:"CommentCount,attr" json:"CommentCount,omitempty"`
	AnswerCount           int           `xml:"AnswerCount,attr" json:"AnswerCount,omitempty"`
	Tags                  string        `xml:"Tags,attr" json:"Tags,omitempty"`
	Title                 string        `xml:"Title,attr" json:"Title,omitempty"`
	LastActivityDate      string        `xml:"LastActivityDate,attr" json:"LastActivityDate,omitempty"`
	LastEditDate          string        `xml:"LastEditDate,attr" json:"LastEditDate,omitempty"`
	LastEditorDisplayName string        `xml:"LastEditorDisplayName,attr" json:"LastEditorDisplayName,omitempty"`
	LastEditorUserId      string        `xml:"LastEditorUserId,attr" json:"LastEditorUserId,omitempty"`
	OwnerUserId           string        `xml:"OwnerUserId,attr" json:"OwnerUserId,omitempty"`

	RelatedPosts []int `xml:"RelatedPostId,attr" json:"RelatedPostId,omitempty"`

	Comments []Comment `json:"Comments"`
	Answers  []*Answer `json:"Answers"`
}

type Comment struct {
	ID           int    `xml:"Id,attr" json:"Id"`
	PostID       int    `xml:"PostId,attr" json:"PostId,omitempty"`
	Score        string `xml:"Score,attr" json:"Score,omitempty"`
	Text         string `xml:"Text,attr" json:"Text,omitempty"`
	UserID       string `xml:"UserId,attr" json:"UserId,omitempty"`
	CreationDate string `xml:"CreationDate,attr" json:"CreationDate,omitempty"`
}

type Answer struct {
	ID               int           `xml:"Id,attr" json:"Id"`
	PostTypeID       string        `xml:"PostTypeId,attr" json:"PostTypeId,omitempty"`
	ParentID         int           `xml:"ParentId,attr" json:"ParentId,omitempty"`
	CreationDate     string        `xml:"CreationDate,attr" json:"CreationDate,omitempty"`
	Score            string        `xml:"Score,attr" json:"Score,omitempty"`
	Body             template.HTML `xml:"Body,attr" json:"Body,omitempty"`
	CommentCount     int           `xml:"CommentCount,attr" json:"CommentCount,omitempty"`
	LastActivityDate string        `xml:"LastActivityDate,attr" json:"LastActivityDate,omitempty"`
	OwnerUserId      string        `xml:"OwnerUserId,attr" json:"OwnerUserId,omitempty"`

	Comments []Comment `json:"Comments"`
}

func (row *Row) GetQuestion() (*Question, error) {
	q := &Question{
		ID:                    *row.ID,
		AcceptedAnswerID:      *row.AcceptedAnswerID,
		CreationDate:          row.CreationDate,
		Score:                 row.Score,
		ViewCount:             row.ViewCount,
		Body:                  template.HTML(row.Body),
		CommunityOwnedDate:    row.CommunityOwnedDate,
		FavoriteCount:         getInt(row.FavoriteCount),
		CommentCount:          getInt(row.CommentCount),
		AnswerCount:           getInt(row.AnswerCount),
		Tags:                  row.Tags,
		Title:                 row.Title,
		LastActivityDate:      row.LastActivityDate,
		LastEditDate:          row.LastEditDate,
		LastEditorDisplayName: row.LastEditorDisplayName,
		LastEditorUserId:      row.LastEditorUserId,
		OwnerUserId:           row.OwnerUserId,
		RelatedPosts:          []int{},
		Comments:              []Comment{},
		Answers:               []*Answer{},
	}

	if row.DeltaType != PostType || row.PostTypeID != "1" {
		return q, fmt.Errorf("row not of type Question")
	}

	return q, nil
}

func (row *Row) GetAnswer() (*Answer, error) {
	a := &Answer{
		ID:               *row.ID,
		CreationDate:     row.CreationDate,
		Score:            row.Score,
		Body:             template.HTML(row.Body),
		CommentCount:     *row.CommentCount,
		LastActivityDate: row.LastActivityDate,
		Comments:         []Comment{},
	}

	if row.DeltaType != PostType || row.PostTypeID != "2" {
		return a, fmt.Errorf("row not of type Answer")
	}

	return a, nil
}

func (row *Row) GetComment() (Comment, error) {
	c := Comment{
		ID:           *row.ID,
		PostID:       *row.PostID,
		Score:        row.Score,
		Text:         row.Text,
		UserID:       row.UserID,
		CreationDate: row.CreationDate,
	}

	if row.DeltaType != CommentsType {
		return c, fmt.Errorf("row not of type Comment")
	}

	return c, nil
}

func (q *Question) AppendRow(r *Row) error {
	switch r.DeltaType {
	case CommentsType:
		c, err := r.GetComment()
		if err != nil {
			return err
		}
		q.AppendComment(c)
		return nil

	case PostType:
		a, err := r.GetAnswer()
		if err != nil {
			return err
		}
		q.AppendAnswer(a)
		return nil
	}
	return fmt.Errorf("row unsupported time at this time")
}

func (q *Question) AppendAnswer(a *Answer) error {
	q.Answers = append(q.Answers, a)
	return nil // add error checking
}

func (q *Question) AppendComment(c Comment) error {
	if q.ID == c.PostID {
		q.Comments = append(q.Comments, c)
		return nil
	}
	for i := range q.Answers {
		if q.Answers[i].ID == c.PostID {
			q.Answers[i].Comments = append(q.Answers[i].Comments, c)
			return nil
		}
	}
	return fmt.Errorf("Comment does not below here")
}

func getInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}
