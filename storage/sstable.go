package storage

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

type SSTable struct {
	Blocks *[]byte
}

func createSSTableFromMemtable(memtable *Memtable, blockSize int) (*SSTable, error) {
	currentBlock := []byte{}
	blocks := []byte{}

	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(MemtableEntry)

		size, serialized_entry := entry.serialize()
		//Check to see if there is enough place in the block to add the entry
		if size > (blockSize - len(currentBlock)) {
			if size > blockSize {
				//Will never fit
				return &SSTable{}, errors.New("entry larger than max block size")
			}
			//Pad remainder of block
			padding := blockSize - len(currentBlock)
			currentBlock = append(currentBlock, make([]byte, padding)...)
			//Prepare new block
			blocks = append(blocks, currentBlock...)
			currentBlock = []byte{}
		}

		currentBlock = append(currentBlock, serialized_entry...)
	}

	//Pad remainder of block
	padding := blockSize - len(currentBlock)
	currentBlock = append(currentBlock, make([]byte, padding)...)
	blocks = append(blocks, currentBlock...)

	return &SSTable{Blocks: &blocks}, nil
}

func (table *SSTable) bytes() []byte {
	return *table.Blocks
}

func searchInSSTable(reader *bufio.Reader, searchId []byte) (*MemtableEntry, error) {
	for {
		idSize := make([]byte, 1)
		_, err := reader.Read(idSize)

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return &MemtableEntry{}, err
		}

		if idSize[0] == byte(0) {
			continue
		}

		id := make([]byte, int(idSize[0]))
		contentLength := make([]byte, 1)

		_, err = reader.Read(id)
		if err != nil {
			return &MemtableEntry{}, err
		}

		_, err = reader.Read(contentLength)
		if err != nil {
			return &MemtableEntry{}, err
		}

		if !bytes.Equal(id, searchId) {
			reader.Discard(int(contentLength[0]))
			continue
		}

		content := make([]byte, contentLength[0])
		_, err = reader.Read(content)

		if err != nil {
			return &MemtableEntry{}, err
		}

		all := []byte{}
		all = append(all, idSize...)
		all = append(all, id...)
		all = append(all, contentLength...)
		all = append(all, content...)

		entry := &MemtableEntry{}
		entry.deserialize(all)
		return entry, nil
	}
}
