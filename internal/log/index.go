package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	OFF_WIDTH uint64 = 4
	POS_WIDTH uint64 = 8
	ENT_WIDTH        = POS_WIDTH + OFF_WIDTH
)

/*
	Persisted memory-mapped files are memory-mapped files that are associated with a source file on a disk. When the last process
	has finished working with the file, the data is saved to the source file on disk. These memory-mapped files are suitable for working
	with extremely large source files.
*/

type index struct {
	file *os.File    // physical file
	mmap gommap.MMap // memory mapped file
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil { // MaxIndexBytes must be a multiple of 12 (4 offset bytes + 8 position bytes)
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// shortcut for returning the last record
	if in == -1 {
		out = uint32((i.size / ENT_WIDTH) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * ENT_WIDTH // moves forward n record, where n = pos
	if i.size < pos+ENT_WIDTH {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+OFF_WIDTH])           // required because of our ability to pass-in -1; otherwise, this could be the same as in
	pos = enc.Uint64(i.mmap[pos+OFF_WIDTH : pos+ENT_WIDTH]) // read the position in the store (after the offset bytes)
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// if we have no more room to Write a new record, send back an EOF error
	if uint64(len(i.mmap)) < i.size+ENT_WIDTH {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+OFF_WIDTH], off)
	enc.PutUint64(i.mmap[i.size+OFF_WIDTH:i.size+ENT_WIDTH], pos)
	i.size += uint64(ENT_WIDTH) // assign the new size of our index (always +12 bytes for each new record we add)
	return nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
