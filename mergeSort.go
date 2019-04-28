package sofp

type iterable interface {
	Next() *Row
}

type merger struct {
	aQueue   iterable
	bQueue   iterable
	outQueue chan *Row
}

func NewMerger(aQueue, bQueue iterable) *merger {
	m := &merger{
		aQueue:   aQueue,
		bQueue:   bQueue,
		outQueue: make(chan *Row),
	}
	go m.readQueues()

	return m
}

func (m *merger) Next() *Row {
	return <-m.outQueue
}

func (m *merger) readQueues() {
	defer close(m.outQueue)

	a := m.aQueue.Next()
	b := m.bQueue.Next()

	for {
		// if both are nil both queues are empty
		if a == nil && b == nil {
			return
		}

		// b queue is empty OR a is older
		if b == nil || (a != nil && a.CreationDate <= b.CreationDate) {
			m.outQueue <- a
			a = m.aQueue.Next()
			continue
		}

		// a is empty OR b is older
		m.outQueue <- b
		b = m.bQueue.Next()

	}
}
