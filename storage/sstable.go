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

type SSTableWriter struct {
	writer *bufio.Writer
}

type SSTableReader struct {
	reader *bufio.Reader
}

func newSSTableReader(path string) SSTableReader {
	fd, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return SSTableReader{
		reader: bufio.NewReader(fd),
	}
}

func newSSTableWriter(path string) SSTableWriter {
	fd, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	return SSTableWriter{
		writer: bufio.NewWriter(fd),
	}
}

func (reader *SSTableReader) peekNextId() ([]byte, error) {

	var idSize []byte

	for {
		var err error

		idSize, err = reader.reader.Peek(1)

		if len(idSize) == 0 || idSize[0] == byte(0) {
			_, err = reader.reader.Read(make([]byte, 1))
		}

		if err != nil {
			return nil, err
		}

		if idSize[0] != byte(0) {
			break
		}
	}

	readLength := int(idSize[0]) + 1

	content, err := reader.reader.Peek(readLength)
	id := content[1:]

	if err != nil {
		return nil, err
	}

	return id, nil
}

func (reader *SSTableReader) readNextEntry() (MemtableEntry, error) {
	idSize := make([]byte, 1)

	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
		_, err := reader.reader.Read(idSize)

		if err != nil {
			return MemtableEntry{}, err
		}

		if idSize[0] != byte(0) {
			break
		}
	}

	id := make([]byte, int(idSize[0]))
	contentLength := make([]byte, 1)

	_, err := reader.reader.Read(id)
	if err != nil {
		return MemtableEntry{}, err
	}

	_, err = reader.reader.Read(contentLength)
	if err != nil {
		return MemtableEntry{}, err
	}

	content := make([]byte, contentLength[0])
	_, err = reader.reader.Read(content)

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

func checkEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func compactSSTables(table1_path string, table2_path string, output_path string) {
	reader1 := newSSTableReader(table1_path)
	reader2 := newSSTableReader(table2_path)

	var id1 []byte
	var id2 []byte

	id1, err1 := reader1.peekNextId()
	id2, err2 := reader2.peekNextId()

	if checkEOF(err1) || checkEOF(err2) {
		return
	}

	var remainder *SSTableReader

	for {
		//id1 is larger than id2
		if bytes.Compare(id1, id2) == 1 {
			entry, err := reader2.readNextEntry()
			panicIfErr(err)
			fmt.Printf("Reading from file2, got entry: %v\n", entry)

			id2, err = reader2.peekNextId()
			fmt.Printf("id2: %i\n", id2)
			if checkEOF(err) {
				remainder = &reader1
				break
			}
		} else {
			entry, err := reader1.readNextEntry()
			fmt.Printf("Reading from file1, got entry: %v\n", entry)
			panicIfErr(err)
			id1, err = reader1.peekNextId()
			fmt.Printf("id1: %i\n", id2)
			if checkEOF(err) {
				remainder = &reader2
				break
			}
		}
	}

	for {
		entry, err := remainder.readNextEntry()
		if checkEOF(err) {
			break
		}
		fmt.Printf("Reading from remainder, got entry: %v\n", entry)
	}
}

func (writer *SSTableWriter) writeFromMemtable(memtable *Memtable) error {
	blockSize := 100
	currentBlock := []byte{}
	blocks := []byte{}

	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(MemtableEntry)

		size, serialized_entry := entry.serialize()
		//Check to see if there is enough place in the block to add the entry
		if size > (blockSize - len(currentBlock)) {
			if size > blockSize {
				//Will never fit
				return errors.New("entry larger than max block size")
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

	_, err := writer.writer.Write(blocks)
	if err != nil {
		return err
	}
	writer.writer.Flush()

	return nil
}

func scanSSTable(path string, searchId []byte) (*MemtableEntry, error) {
	reader := newSSTableReader(path)
	for {
		entry, err := reader.readNextEntry()
		if checkEOF(err) {
			return nil, nil
		}
		if !bytes.Equal(entry.id, searchId) {
			continue
		}

		return &entry, nil
	}
}
