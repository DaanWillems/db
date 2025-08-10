package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type SSTableIterator struct {
	blockIterator *SSTableBlockIterator
	entryIterator *SSTableEntryIterator
	lastEntry     *Entry
}

func NewSSTableIterator(reader *SSTableReader) *SSTableIterator {
	return &SSTableIterator{
		blockIterator: NewSSTableBlockIterator(reader),
	}
}

func NewSSTableIteratorFromPath(path string) *SSTableIterator {
	return &SSTableIterator{
		blockIterator: NewSSTableBlockIteratorFromPath(path),
	}
}

func (it *SSTableIterator) Entry() *Entry {
	return it.lastEntry
}

func (it *SSTableIterator) Next() bool {
	if it.entryIterator == nil {
		if result := it.blockIterator.Next(); !result {
			return false
		}
		it.entryIterator = NewSSTableEntryIterator(it.blockIterator.Block())
	}

	foundEntry := it.entryIterator.Next()
	if !foundEntry {
		if result := it.blockIterator.Next(); !result {
			return false
		} else {
			it.entryIterator = NewSSTableEntryIterator(it.blockIterator.Block())
			if foundEntry := it.entryIterator.Next(); !foundEntry {
				return false
			}
		}
	}

	it.lastEntry = it.entryIterator.Entry()
	if it.lastEntry.id == nil {
		fmt.Printf("\n")
	}
	return true
}

type SSTableEntryIterator struct {
	entryIndex int
	lastEntry  *Entry
	error      error
	buffer     *bytes.Buffer
}

func NewSSTableEntryIterator(buffer *bytes.Buffer) *SSTableEntryIterator {
	return &SSTableEntryIterator{
		buffer: buffer,
	}
}

func (it *SSTableEntryIterator) Err() error {
	return it.error
}

func (it *SSTableEntryIterator) Entry() *Entry {
	return it.lastEntry
}

func (it *SSTableEntryIterator) Next() bool {
	entry := Entry{}
	if it.buffer.Available() == 0 {
		return false
	}
	err := entry.deserialize(it.buffer)
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		it.error = err
		return false
	}
	it.lastEntry = &entry
	it.entryIndex += 1
	return true
}

type SSTableBlockIterator struct {
	blockIndex int
	lastBlock  *bytes.Buffer
	error      error
	reader     *SSTableReader
}

func NewSSTableBlockIteratorFromPath(path string) *SSTableBlockIterator {
	reader := newSSTableReaderFromPath(path)
	return &SSTableBlockIterator{
		reader: &reader,
	}
}

func NewSSTableBlockIterator(reader *SSTableReader) *SSTableBlockIterator {
	return &SSTableBlockIterator{
		reader: reader,
	}
}

func (it *SSTableBlockIterator) Err() error {
	return it.error
}

func (it *SSTableBlockIterator) Block() *bytes.Buffer {
	return it.lastBlock
}
func (it *SSTableBlockIterator) Next() bool {
	block, err := it.reader.getBlock(it.blockIndex)
	if errors.Is(err, BlockOutOfBounds) {
		return false
	} else if err != nil {
		it.error = err
		return false
	}
	it.blockIndex++
	it.lastBlock = block
	return true
}

type SSTableReader struct {
	file     *os.File
	fileSize int64
}

func newSSTableReaderFromPath(path string) SSTableReader {
	fd, err := fileManager.openReadFile(path)
	panicIfErr(err)
	fileInfo, err := fd.Stat()
	panicIfErr(err)
	return SSTableReader{
		fileSize: fileInfo.Size(),
		file:     fd,
	}
}

func (reader *SSTableReader) getBlock(block int) (*bytes.Buffer, error) {
	offset := int64(block * config.BlockSize)
	readAmount := config.BlockSize

	if offset > reader.fileSize {
		return nil, BlockOutOfBounds
	}

	if int64(config.BlockSize) > (reader.fileSize - offset) {
		readAmount = int(reader.fileSize - offset)
	}

	blockBytes := make([]byte, readAmount)
	_, err := reader.file.ReadAt(blockBytes, offset)

	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(blockBytes)
	return buffer, nil
}
