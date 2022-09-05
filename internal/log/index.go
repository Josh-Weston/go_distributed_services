package log

import (
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
