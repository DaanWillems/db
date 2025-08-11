package storage

import (
	"bytes"
	"errors"
	"io"
)

type SSTableIterator struct {
	blockIterator *SSTableBlockIterator
	lastEntry     *Entry
	lastBlock     *bytes.Buffer
	error         error
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

func (it *SSTableIterator) Error() error {
	return it.error
}

func (it *SSTableIterator) Next() bool {
	//Load the first block
	if it.lastBlock == nil {
		//If the first block does not exist, exit immediately
		if result := it.blockIterator.Next(); !result {
			return false
		} else {
			it.lastBlock = it.blockIterator.Block()
		}
	}

	entry := Entry{}
	err := entry.deserialize(it.lastBlock)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		//Try loading next block
		if result := it.blockIterator.Next(); !result {
			return false
		} else {
			it.lastBlock = it.blockIterator.Block()
			err := entry.deserialize(it.lastBlock)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return false
			} else if err != nil {
				it.error = err
				return false
			}
		}
	} else if err != nil {
		it.error = err
		return false
	}

	if err = it.blockIterator.Error(); err != nil {
		it.error = err
		return false
	}

	it.lastEntry = &entry
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

func (it *SSTableBlockIterator) Error() error {
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
