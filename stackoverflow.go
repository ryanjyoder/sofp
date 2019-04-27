package sofp

type archiveParser struct {
	q            iterable
	streamLookup map[string]string
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

	postPlusHistory := NewMerger(postPsr, historyPsr)
	postsHistoryComments := NewMerger(postPlusHistory, commentsPsr)
	metadata := NewMerger(postLinkPsr, votesPsr)
	merged := NewMerger(postsHistoryComments, metadata)

	return &archiveParser{
		q:            merged,
		streamLookup: map[string]string{},
	}, nil

}

func (p *archiveParser) Next() *Row {
	row := p.q.Next()
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
	if row.DeltaType == "Post" {
		if row.PostTypeID == "1" {
			row.PostID = row.ID
			p.streamLookup[row.PostID] = row.PostID
		}
		if row.PostTypeID == "2" {
			p.streamLookup[row.PostID] = row.ParentID
		}
	}

	row.StreamID = p.streamLookup[row.PostID]

	return row
}
