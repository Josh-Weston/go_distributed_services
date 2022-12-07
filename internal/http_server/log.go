package server

import (
	"fmt"
	"sync"
)

// This is an append-only, in-memory log, setup as a slice
// The Record.Offset field is simply the index of the record in the slice

type Record struct {
	Value  []byte `json:"value"` // encoding/json will encode []byte as base64
	Offset uint64 `json:"offset"`
}

type Log struct {
	mu      sync.RWMutex // no need to use a pointer because the struct is a pointer
	records []Record
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
