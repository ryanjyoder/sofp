package sofp

type readUntil struct {
	q       iterable
	current *Row
	untilID int
}

func NewReadUntil(q iterable) *readUntil {
	return &readUntil{
		q:       q,
		current: q.Next(),
	}
}

func (r *readUntil) HasNext() bool {
	if r.current == nil {
		return false
	}

	return *r.current.PostID <= r.untilID
}
func (r *readUntil) ReadUntil(unitID int) {
	r.untilID = unitID
}

func (r *readUntil) Next() *Row {
	result := r.current
	r.current = r.q.Next()
	return result
}
