package main

import (
	"errors"
)

type SSTable struct {
  Blocks *[]byte
}

func CreateSSTableFromMetable(memtable *Memtable) (*SSTable, error) {
	blockSize := 11 // 4 bytes
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
			for range blockSize - len(currentBlock) {
				currentBlock = append(currentBlock, byte(0))
			}
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
