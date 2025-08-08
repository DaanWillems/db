package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
}

type SSTableReader struct {
	buffer    *bufio.Reader
	rawBuffer *bytes.Buffer
	file      *os.File
}

func newSSTableReader(rawBuffer *bytes.Buffer) SSTableReader {
	return SSTableReader{
		buffer:    bufio.NewReader(rawBuffer),
		rawBuffer: rawBuffer,
	}
}

func newSSTableReaderFromPath(path string) SSTableReader {
	fd, err := os.Open(path)
	panicIfErr(err)
	return SSTableReader{
		buffer: bufio.NewReader(fd),
		file:   fd,
	}
}

func newSSTableWriter(buffer *bufio.Writer) SSTableWriter {
	return SSTableWriter{
		buffer:          buffer,
		currentBlockLen: 0,
	}
}

func newSSTableWriterFromPath(path string) SSTableWriter {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	panicIfErr(err)
	return SSTableWriter{
		buffer:          bufio.NewWriter(fd),
		currentBlockLen: 0,
	}
}

func (reader *SSTableReader) peekNextId() ([]byte, error) {

	pos := 1
	var idSize int

	for {
		var result []byte
		result, err := reader.buffer.Peek(pos)
		if checkEOF(err) {
			return nil, err
		}
		if result[len(result)-1] == byte(0) {
			pos += 1
			continue
		}
		idSize = int(result[len(result)-1])
		break
	}

	content, err := reader.buffer.Peek(pos + idSize)
	id := content[pos:]

	if err != nil {
		return nil, err
	}

	return id, nil
}

func (reader *SSTableReader) readNextEntry() (Entry, error) {
	blockSize := 100

	idSize := make([]byte, 1)

	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
		_, err := reader.buffer.Read(idSize)

		if err != nil {
			return Entry{}, err
		}

		if idSize[0] != byte(0) {
			//Try to read next block into buffer
			reader.buffer.Peek(blockSize)
			break
		} else {
			continue
		}
	}

	id := make([]byte, int(idSize[0]))
	_, err := reader.buffer.Read(id)
	if err != nil {
		return Entry{}, err
	}

	deleted := make([]byte, 1)
	_, err = reader.buffer.Read(deleted)
	if err != nil {
		return Entry{}, err
	}

	valueLength := make([]byte, 1)
	_, err = reader.buffer.Read(valueLength)
	if err != nil {
		return Entry{}, err
	}

	value := make([]byte, valueLength[0])
	_, err = reader.buffer.Read(value)

	if err != nil {
		return Entry{}, err
	}

	all := []byte{}
	all = append(all, idSize...)
	all = append(all, id...)
	all = append(all, deleted...)
	all = append(all, valueLength...)
	all = append(all, value...)

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

func (reader *SSTableReader) reset() {
	if reader.rawBuffer != nil {
		reader.buffer = bufio.NewReader(bytes.NewReader(reader.rawBuffer.Bytes()))
	} else if reader.file != nil {
		reader.file.Seek(0, io.SeekStart)
		reader.buffer = bufio.NewReader(reader.file)
	}
}

// Method for testing, fully scans the table and returns the number of entires
func (reader *SSTableReader) count() int {
	count := 0
	reader.reset()
	for {
		_, err := reader.readNextEntry()
		if checkEOF(err) {
			return count
		}
		count++
	}
}

func (reader *SSTableReader) scan(searchId []byte) (*Entry, error) {
	reader.reset()
	for {
		entry, err := reader.readNextEntry()

		if checkEOF(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(entry.id, searchId) {
			continue
		}
		return &entry, nil
	}
}
