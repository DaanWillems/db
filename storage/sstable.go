package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type SSTable struct {
	Blocks *[]byte
}

type SSTableReader struct {
	reader *bufio.Reader
}

func (sstablereader *SSTableReader) peekNextId() ([]byte, error) {
	return nil, nil
}

func (sstablereader *SSTableReader) readNextEntry() ([]byte, error) {
	return nil, nil
}

func checkEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func compactSSTables(table1_path string, table2_path string, output_path string) {
	fd1, err := os.Open(table1_path)
	if err != nil {
		panic(err)
	}
	reader1 := bufio.NewReader(fd1)
	fd2, err := os.Open(table2_path)
	if err != nil {
		panic(err)
	}
	reader2 := bufio.NewReader(fd2)

	entry1, err1 := readEntryFromSSTable(reader1)
	entry2, err2 := readEntryFromSSTable(reader2)

	if checkEOF(err1) || checkEOF(err2) {
		return
	}

	var remainder *bufio.Reader

	for {
		//entry1 is larger than entry2
		if bytes.Compare(entry1.id, entry2.id) == 1 {
			//write entry2.id
			entry2, err = readEntryFromSSTable(reader2)
			if checkEOF(err) {
				remainder = reader1
				break
			}

		} else {
			//write entry1.id
			entry1, err = readEntryFromSSTable(reader1)
			if checkEOF(err) {
				remainder = reader2
				break
			}
		}
	}

	for {
		entry, err := readEntryFromSSTable(remainder)
		fmt.Printf("%v", entry)
		if checkEOF(err) {
			break
		}
	}
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

func readEntryFromSSTable(reader *bufio.Reader) (MemtableEntry, error) {
	idSize := make([]byte, 1)

	for {
		_, err := reader.Read(idSize)

		if err != nil {
			return MemtableEntry{}, err
		}

		if idSize[0] != byte(0) {
			break
		}
	}

	id := make([]byte, int(idSize[0]))
	contentLength := make([]byte, 1)

	_, err := reader.Read(id)
	if err != nil {
		return MemtableEntry{}, err
	}

	_, err = reader.Read(contentLength)
	if err != nil {
		return MemtableEntry{}, err
	}

	content := make([]byte, contentLength[0])
	_, err = reader.Read(content)

	if err != nil {
		return MemtableEntry{}, err
	}

	all := []byte{}
	all = append(all, idSize...)
	all = append(all, id...)
	all = append(all, contentLength...)
	all = append(all, content...)

	entry := MemtableEntry{}
	entry.deserialize(all)
	return entry, nil
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
