package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
}

type SSTableReader struct {
	reader     *bufio.Reader
	bytes_read []byte
}

func newSSTableReader(buffer *bufio.Reader) SSTableReader {
	return SSTableReader{
		reader:     buffer,
		bytes_read: []byte{},
	}
}

func newSSTableReaderFromPath(path string) SSTableReader {
	fd, err := os.Open(path)
	panicIfErr(err)
	return newSSTableReader(bufio.NewReaderSize(fd, 1024*1024))
}

func newSSTableWriter(buffer *bufio.Writer) SSTableWriter {
	return SSTableWriter{
		buffer:          buffer,
		currentBlockLen: 0,
	}
}

func newSSTableWriterFromPath(path string) (*os.File, SSTableWriter) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	panicIfErr(err)
	return fd, newSSTableWriter(bufio.NewWriter(fd))
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

func (reader *SSTableReader) readNextEntry() (Entry, error) {
	idSize := make([]byte, 1)

	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
		_, err := reader.reader.Read(idSize)
		reader.bytes_read = append(reader.bytes_read, idSize...)

		if err != nil {
			return Entry{}, err
		}

		if idSize[0] != byte(0) {
			break
		} else {
			continue
		}
	}

	id := make([]byte, int(idSize[0]))
	_, err := reader.reader.Read(id)
	if err != nil {
		return Entry{}, err
	}

	deleted := make([]byte, 1)
	_, err = reader.reader.Read(deleted)
	if err != nil {
		return Entry{}, err
	}

	valueLength := make([]byte, 1)
	_, err = reader.reader.Read(valueLength)
	if err != nil {
		return Entry{}, err
	}

	value := make([]byte, valueLength[0])
	_, err = reader.reader.Read(value)

	if err != nil {
		return Entry{}, err
	}

	all := []byte{}
	all = append(all, idSize...)
	all = append(all, id...)
	all = append(all, deleted...)
	all = append(all, valueLength...)
	all = append(all, value...)

	reader.bytes_read = append(reader.bytes_read, all...)

	entry := Entry{}
	entry.deserialize(all)
	return entry, nil
}

func (writer *SSTableWriter) writeSingleEntry(entry *Entry) error {
	blockSize := 100

	size, serialized_entry := entry.serialize()
	//Check to see if there is enough place in the block to add the entry
	if size > (blockSize - writer.currentBlockLen) {
		if size > blockSize {
			//Will never fit
			return errors.New("entry larger than max block size")
		}

		//Pad remainder of block
		padding := blockSize - writer.currentBlockLen
		// log.Printf("About to write padding %v", padding)

		_, err := writer.buffer.Write(make([]byte, padding))
		panicIfErr(err)

		writer.currentBlockLen = 0
	}

	writer.currentBlockLen += size
	_, err := writer.buffer.Write(serialized_entry)
	panicIfErr(err)
	writer.buffer.Flush()
	return nil
}

func (writer *SSTableWriter) writeFromMemtable(memtable *Memtable) error {
	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(Entry)
		writer.writeSingleEntry(&entry)
	}
	return nil
}

func scanSSTable(buffer *bufio.Reader, searchId []byte) (*Entry, error) {
	reader := newSSTableReader(buffer)
	count := 0
	for {
		if count == 368 {
			fmt.Printf("\n")
		}
		count++
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
