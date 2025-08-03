package storage

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"os"
)

type SSTable struct {
	Blocks *[]byte
}

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int
}

type SSTableReader struct {
	reader *bufio.Reader
}

func newSSTableReader(buffer *bufio.Reader) SSTableReader {
	return SSTableReader{
		reader: buffer,
	}
}

func newSSTableWriter(buffer *bufio.Writer) SSTableWriter {
	return SSTableWriter{
		buffer:          buffer,
		currentBlockLen: 0,
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
	fd1, err := os.Open(table1_path)
	panicIfErr(err)
	fd2, err := os.Open(table2_path)
	panicIfErr(err)
	fd3, err := os.OpenFile(output_path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 644)
	panicIfErr(err)

	buffer1 := bufio.NewReader(fd1)
	buffer2 := bufio.NewReader(fd2)
	buffer3 := bufio.NewWriter(fd3)

	reader1 := newSSTableReader(buffer1)
	reader2 := newSSTableReader(buffer2)
	writer := newSSTableWriter(buffer3)

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
			writer.writeSingleEntry(&entry)
			id2, err = reader2.peekNextId()
			if checkEOF(err) {
				remainder = &reader1
				break
			}
		} else {
			entry, err := reader1.readNextEntry()
			panicIfErr(err)
			writer.writeSingleEntry(&entry)
			id1, err = reader1.peekNextId()
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

		writer.writeSingleEntry(&entry)
	}
}

func (writer *SSTableWriter) writeSingleEntry(entry *MemtableEntry) error {
	blockSize := 100
	size, serialized_entry := entry.serialize()
	log.Println("Writing single entry")
	//Check to see if there is enough place in the block to add the entry
	if size > (blockSize - writer.currentBlockLen) {
		if size > blockSize {
			//Will never fit
			return errors.New("entry larger than max block size")
		}

		log.Println("Padding block..")
		//Pad remainder of block
		padding := blockSize - writer.currentBlockLen
		writer.buffer.Write(make([]byte, padding))

		writer.currentBlockLen = 0
	}

	log.Println("Writing entry..")
	writer.currentBlockLen += size
	_, err := writer.buffer.Write(serialized_entry)
	panicIfErr(err)
	writer.buffer.Flush()
	return nil
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

	_, err := writer.buffer.Write(blocks)
	if err != nil {
		return err
	}
	writer.buffer.Flush()

	return nil
}

func scanSSTable(buffer *bufio.Reader, searchId []byte) (*MemtableEntry, error) {
	reader := newSSTableReader(buffer)
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
