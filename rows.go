package sofp

import (
	"fmt"
	"html/template"

	"github.com/golang-commonmark/markdown"
)

const (
	PostsType       = "Posts"
	PostHistoryType = "PostHistory"
	CommentsType    = "Comments"
	PostLinksType   = "PostLinks"
	VotesType       = "Votes"
)

var (
	DeltaTypeOrder = []string{PostHistoryType, CommentsType, PostLinksType, VotesType}
)

func (r *Row) GetStreamID() string {
	return r.StreamID
}
func (r *Row) GetID() string {
	return fmt.Sprintf("%s-%s-%d", r.StreamID, r.DeltaType, *r.ID)
}

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
	OwnerUserId           string `xml:"OwnerUserId,attr" json:"OwnerUserId,omitempty"`
	LastEditorUserId      string `xml:"LastEditorUserId,attr" json:"LastEditorUserId,omitempty"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" json:"LastEditorDisplayName,omitempty"`
	LastEditDate          string `xml:"LastEditDate,attr" json:"LastEditDate,omitempty"`
	LastActivityDate      string `xml:"LastActivityDate,attr" json:"LastActivityDate,omitempty"`
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr" json:"CommunityOwnedDate,omitempty"`
	ClosedDate            string `xml:"ClosedDate,attr" json:"ClosedDate,omitempty"`
	Title                 string `xml:"Title,attr" json:"Title,omitempty"`
	Tags                  string `xml:"Tags,attr" json:"Tags,omitempty"`
	AnswerCount           *int   `xml:"AnswerCount,attr" json:"AnswerCount,omitempty"`
	CommentCount          *int   `xml:"CommentCount,attr" json:"CommentCount,omitempty"`
	FavoriteCount         *int   `xml:"FavoriteCount,attr" json:"FavoriteCount,omitempty"`

	// PostHistory Attributes
	PostHistoryTypeID string `xml:"PostHistoryTypeId,attr" json:"PostHistoryTypeId,omitempty"`
	PostID            *int   `xml:"PostId,attr" json:"PostId,omitempty"`
	RevisionGUID      string `xml:"RevisionGUID,attr" json:"RevisionGUID,omitempty"`
	UserID            string `xml:"UserId,attr" json:"UserId,omitempty"`
	Comment           string `xml:"Comment,attr" json:"Comment,omitempty"`
	Text              string `xml:"Text,attr" json:"Text,omitempty"`
	UserDisplayName   string `xml:"UserDisplayName,attr" json:"UserDisplayName,omitempty"`
	CloseReasonID     *int   `xml:"CloseReasonId,attr" json:"CloseReasonId,omitempty"`

	// No Comments only attributes

	// PostLinks only attributes
	RelatedPostID string `xml:"RelatedPostId,attr" json:"RelatedPostId,omitempty"`
	LinkTypeID    string `xml:"LinkTypeId,attr" json:"LinkTypeId,omitempty"`

	// Votes
	VoteTypeId   int      `xml:"VoteTypeId,attr" json:"VoteTypeId,omitempty"`
	BountyAmount *float64 `xml:"BountyAmount,attr" json:"BountyAmount,omitempty"`

	DeltaType string `json:"DeltaType"`
	StreamID  string
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

	if row.PostTypeID != "1" {
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

	if row.DeltaType != PostsType || row.PostTypeID != "2" {
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

	case PostsType:
		a, err := r.GetAnswer()
		if err != nil {
			return err
		}
		q.AppendAnswer(a)
		return nil
	case PostHistoryType:
		q.AppendHistory(r)
		return nil
	case PostLinksType:
		return q.AppendLink(r)

	case VotesType:
		return q.AppendVote(r)
	}
	return fmt.Errorf("row unsupported time at this time: %s", r.DeltaType)
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

func (q *Question) AppendVote(r *Row) error {
	// ignored currently
	return nil
}

func (q *Question) AppendLink(r *Row) error {
	// ignored currently
	return nil
}

func (q *Question) AppendHistory(r *Row) error {
	if r.DeltaType != PostHistoryType {
		return fmt.Errorf("row not history type: %v", r.DeltaType)
	}
	if q.ID == 0 && r.PostID != nil {
		q.ID = *r.PostID
	}

	if r.PostID != nil && *r.PostID != q.ID {
		// answer
		return q.findAnswer(*r.PostID).AppendHistory(r)
	}

	switch r.PostHistoryTypeID {
	// Title //
	case "1": //= Initial Title - initial title (questions only)
		fallthrough
	case "4": //Edit Title - modified title (questions only)
		fallthrough
	case "7": //Rollback Title - reverted title (questions only)
		q.Title = r.Text

		// Body //
	case "2": // Initial Body - initial post raw body text
		fallthrough
	case "5": //Edit Body - modified post body (raw markdown)
		fallthrough
	case "8": //Rollback Body - reverted body (raw markdown)
		q.Body = template.HTML(markdown.New().RenderToString([]byte(r.Text)))

		// Tags //
	case "3": //Initial Tags - initial list of tags (questions only)
		fallthrough
	case "6": //Edit Tags - modified list of tags (questions only)
		fallthrough
	case "9": //Rollback Tags - reverted list of tags (questions only)
		q.Tags = r.Text

		//
	case "10": //Post Closed - post voted to be closed
	case "11": //Post Reopened - post voted to be reopened
	case "12": //Post Deleted - post voted to be removed
	case "13": //Post Undeleted - post voted to be restored
	case "14": //Post Locked - post locked by moderator
	case "15": //Post Unlocked - post unlocked by moderator
	case "16": //Community Owned - post now community owned
	case "17": //Post Migrated - post migrated - now replaced by 35/36 (away/here)
	case "18": //Question Merged - question merged with deleted question
	case "19": //Question Protected - question was protected by a moderator.
	case "20": //Question Unprotected - question was unprotected by a moderator.
	case "21": //Post Disassociated - OwnerUserId removed from post by admin
	case "22": //Question Unmerged - answers/votes restored to previously merged question
	case "24": //Suggested Edit Applied
	case "25": //Post Tweeted
	case "31": //Comment discussion moved to chat
	case "33": //Post notice added - comment contains foreign key to PostNotices
	case "34": //Post notice removed - comment contains foreign key to PostNotices
	case "35": //Post migrated away - replaces id 17
	case "36": //Post migrated here - replaces id 17
	case "37": //Post merge source
	case "38": //Post merge destination
	case "50": //Bumped by Community User
	case "52": //Question became hot network question
	case "53": //Question removed from hot network questions by a moderator
	default:
		return fmt.Errorf("PostHistoryTypeID not recognized: %s", r.PostHistoryTypeID)
	}
	return nil
}

func (q *Question) findAnswer(postID int) *Answer {
	for i := range q.Answers {
		if q.Answers[i].ID == postID {
			return q.Answers[i]
		}
	}

	// if not found create it and append to list of answers
	a := &Answer{
		ID: postID,
	}
	q.Answers = append(q.Answers, a)
	return a

}

func (q *Answer) AppendHistory(r *Row) error {
	if r.DeltaType != PostHistoryType {
		return fmt.Errorf("row not history type: %v", r.DeltaType)
	}

	switch r.PostHistoryTypeID {
	// Body //
	case "2": // Initial Body - initial post raw body text
		fallthrough
	case "5": //Edit Body - modified post body (raw markdown)
		fallthrough
	case "8": //Rollback Body - reverted body (raw markdown)
		q.Body = template.HTML(markdown.New().RenderToString([]byte(r.Text)))
	default:
		return fmt.Errorf("PostHistoryTypeID not recognized or valid for answer: %s", r.PostHistoryTypeID)

	}
	return nil
}

func getInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}
