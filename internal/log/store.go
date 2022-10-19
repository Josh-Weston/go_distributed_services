package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const LEN_WIDTH = 8 // number of bytes used to store the records length (8 bytes = 64 bits = 9.2e18 maxium message length)

// a wrapper around a file to append and read bytes from the file
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size()) // incase we are using an existing file (e.g., our service has restarted)
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// messages are stored as [messageLength(64 bits)|Message|messageLength(64 bits)|Message]

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil { // write the length of p so we know how many bytes to read
		return 0, 0, err
	}
	w, err := s.buf.Write(p) // write the contents of p
	if err != nil {
		return 0, 0, err
	}
	w += LEN_WIDTH // always use 8 bytes to denote the length of the message
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock() // could use a RWlock here instead
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil { // prevent reading a record that hasn't been flushed to disk, yet. Not sure why this isn't a part of Append??
		return nil, err
	}
	size := make([]byte, LEN_WIDTH)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil { // read the 8 bytes dedicated to the message length
		return nil, err
	}
	b := make([]byte, enc.Uint64(size)) // decode the size of the message
	if _, err := s.File.ReadAt(b, int64(pos+LEN_WIDTH)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
