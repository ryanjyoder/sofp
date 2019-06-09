package sofp

import (
	"fmt"
	"os"
	"path/filepath"
)

type FDPool struct {
	fds      []*os.File
	fdLookup map[string]int
	nextPos  int
}

func NewFDPool(poolSize int) (*FDPool, error) {
	if poolSize < 1 {
		return nil, fmt.Errorf("pool size must be greater than one")
	}

	return &FDPool{
		fds:      make([]*os.File, poolSize),
		fdLookup: map[string]int{},
	}, nil
}

func (p *FDPool) GetFD(path string) (*os.File, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if fdIndex, ok := p.fdLookup[path]; ok {
		return p.fds[fdIndex], nil
	}

	if p.fds[p.nextPos] != nil {
		p.fds[p.nextPos].Close()
		p.fds[p.nextPos] = nil
		for key, i := range p.fdLookup {
			if i == p.nextPos {
				delete(p.fdLookup, key)
				break
			}
		}
	}

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	p.fdLookup[path] = p.nextPos
	p.fds[p.nextPos] = fd
	p.nextPos = (p.nextPos + 1) % len(p.fds)

	return fd, err
}

func (p *FDPool) CloseAll() error {
	for i := range p.fds {
		if p.fds[i] != nil {
			p.fds[i].Close()
		}
	}
	p.fds = make([]*os.File, len(p.fds))
	p.fdLookup = map[string]int{}
	return nil
}
