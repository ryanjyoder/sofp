package sofp

import (
	"fmt"
	"path/filepath"
)

type archiveParser struct {
	rows         chan *Row
	streamLookup map[int]int
	posts        iterable
	history      *readUntil
	comments     *readUntil
	postLinks    *readUntil
	votes        *readUntil
}

const (
	PostType        = "Posts"
	PostHistoryType = "PostHistory"
	CommentsType    = "Comments"
	PostLinksType   = "PostLinks"
	VotesType       = "Votes"
)

func NewArchiveParser(posts, postHistory, comments, postLinks, votes string) (*archiveParser, error) {
	postPsr, err := NewParser(posts, PostType)
	if err != nil {
		return nil, err
	}

	historyPsr, err := NewParser(postHistory, PostHistoryType)
	if err != nil {
		return nil, err
	}

	commentsPsr, err := NewParser(comments, CommentsType)
	if err != nil {
		return nil, err
	}

	postLinkPsr, err := NewParser(postLinks, PostLinksType)
	if err != nil {
		return nil, err
	}

	votesPsr, err := NewParser(votes, VotesType)
	if err != nil {
		return nil, err
	}

	psr := &archiveParser{
		rows:         make(chan *Row),
		streamLookup: map[int]int{},
		posts:        postPsr,
		history:      NewReadUntil(historyPsr),
		comments:     NewReadUntil(commentsPsr),
		postLinks:    NewReadUntil(postLinkPsr),
		votes:        NewReadUntil(votesPsr),
	}
	go psr.read()

	return psr, nil
}

func (p *archiveParser) read() {
	defer close(p.rows)
	for {
		post := p.posts.Next()
		if post == nil {
			return
		}
		p.rows <- post

		if post.err != nil {
			return
		}

		// PostHistory
		p.history.ReadUntil(*post.ID)
		for p.history.HasNext() {
			p.rows <- p.history.Next()
		}

		// Comments
		p.comments.ReadUntil(*post.ID)
		for p.comments.HasNext() {
			p.rows <- p.comments.Next()
		}

		// PostLinks
		p.postLinks.ReadUntil(*post.ID)
		for p.postLinks.HasNext() {
			p.rows <- p.postLinks.Next()
		}

		// Votes
		p.votes.ReadUntil(*post.ID)
		for p.votes.HasNext() {
			p.rows <- p.votes.Next()
		}

	}
}

func (p *archiveParser) Next() *Row {
	row := <-p.rows
	// EOF
	if row == nil {
		return nil
	}

	// Some parsing error
	if row.err != nil {
		return row
	}

	// The main post ID is the used for the stream id.
	// If it's a post then use the postID, but if it's a reply use the parent id.
	if row.DeltaType == PostType {
		row.PostID = row.ID
		if row.PostTypeID == "1" {
			p.streamLookup[*row.PostID] = *row.PostID
		}
		if row.PostTypeID == "2" {
			p.streamLookup[*row.PostID] = *row.ParentID
		}
	}

	row.Stream = fmt.Sprintf("%d", p.streamLookup[*row.PostID])

	return row
}

func GetFilepathsFromDir(baseDir string) (string, string, string, string, string) {

	return filepath.Join(baseDir, "Posts.xml"),
		filepath.Join(baseDir, "PostHistory.xml"),
		filepath.Join(baseDir, "Comments.xml"),
		filepath.Join(baseDir, "PostLinks.xml"),
		filepath.Join(baseDir, "Votes.xml")
}
