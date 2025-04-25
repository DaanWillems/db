package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
)

type SSTable struct {
	Blocks *[]byte
}

func CreateSSTableFromMetable(memtable *Memtable, blockSize int) (*SSTable, error) {
	currentBlock := []byte{}
	blocks := []byte{}

	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(MemtableEntry)

		size, serialized_entry := entry.Serialize()
		//Check to see if there is enough place in the block to add the entry
		if size > (blockSize - len(currentBlock)) {
			if size > blockSize {
				//Will never fit
				return &SSTable{}, errors.New("Entry larger than max block size")
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
	for range blockSize - len(currentBlock) {
		currentBlock = append(currentBlock, byte(0))
	}

	blocks = append(blocks, currentBlock...)

	return &SSTable{Blocks: &blocks}, nil
}

func (table *SSTable) Flush(path string) error {
	return os.WriteFile(path, *table.Blocks, 0644)
}

func SearchInSSTable(path string, searchId []byte) (MemtableEntry, error) {


	fd, err := os.Open(path)
	if err != nil { //error handler
		return MemtableEntry{}, err
	}

	reader := bufio.NewReader(fd) // creates a new reader

	for {
		idSize := make([]byte, 1)
		_, err = reader.Read(idSize)

		if err != nil {
			return MemtableEntry{}, err
		}

		if idSize[0] == byte(0) {
			continue
		}

		id := make([]byte, int(idSize[0]))
		contentLength := make([]byte, 1)

		_, err = reader.Read(id)
		_, err = reader.Read(contentLength)

		if !bytes.Equal(id, searchId) {
			reader.Discard(int(contentLength[0]))
			continue
		}

		content := make([]byte, contentLength[0])
		_, err := reader.Read(content)

		if err != nil {
			return MemtableEntry{}, err
		}

    all := []byte{}
		all = append(all, idSize...) 
		all = append(all, id...) 
		all = append(all, contentLength...)
		all = append(all, content...)

		fmt.Printf("%v \n", all)
		fmt.Println("")
		entry := MemtableEntry{}
		entry.Deserialize(all)
		return entry, nil
	}
}
