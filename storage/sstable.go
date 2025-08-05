package storage

import (
	"bufio"
	"bytes"
	"errors"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
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

func (reader *SSTableReader) readNextEntry() (Entry, error) {
	idSize := make([]byte, 1)

	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
		_, err := reader.reader.Read(idSize)

		if err != nil {
			return Entry{}, err
		}

		if idSize[0] != byte(0) {
			break
		}
	}

	id := make([]byte, int(idSize[0]))
	contentLength := make([]byte, 1)

	_, err := reader.reader.Read(id)
	if err != nil {
		return Entry{}, err
	}

	_, err = reader.reader.Read(contentLength)
	if err != nil {
		return Entry{}, err
	}

	content := make([]byte, contentLength[0])
	_, err = reader.reader.Read(content)

	if err != nil {
		return Entry{}, err
	}

	all := []byte{}
	all = append(all, idSize...)
	all = append(all, id...)
	all = append(all, contentLength...)
	all = append(all, content...)

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
		writer.buffer.Write(make([]byte, padding))

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
