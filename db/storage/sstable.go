package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
	currentBlock    int //The current block we're writing to
	path            string
}

type SSTableReader struct {
	buffer    *bufio.Reader
	rawBuffer *bytes.Buffer
	file      *os.File
}

func newSSTableReaderFromPath(path string) SSTableReader {
	fd, err := fileManager.openReadFile(path)
	panicIfErr(err)
	return SSTableReader{
		buffer: bufio.NewReader(fd),
		file:   fd,
	}
}

func newSSTableWriterFromPath(path string) SSTableWriter {
	fd, err := fileManager.openWriteFile(path)
	panicIfErr(err)
	return SSTableWriter{
		buffer:          bufio.NewWriter(fd),
		currentBlockLen: 0,
		path:            path,
	}
}

func (reader *SSTableReader) getLastId() ([]byte, error) {
	//Calculate location for last block
	offset := int64(config.BlockSize * (config.SSTableBlockCount - 1))
	content := make([]byte, config.BlockSize)
	reader.file.ReadAt(content, offset)

	buffer := bufio.NewReader(bytes.NewBuffer(content))
	lastEntry := Entry{}
	for { //If the size is 0, the block is done:
		entry := Entry{}
		err := entry.deserialize(buffer)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return lastEntry.id, nil
		}
		if checkEOF(err) {
			return lastEntry.id, nil
		}
		if err != nil {
			return nil, err
		}

		lastEntry = entry
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
	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
		idSize, err := reader.buffer.Peek(1)

		if err != nil {
			return Entry{}, err
		}

		if idSize[0] == byte(0) {
			reader.buffer.ReadByte() //Consume the zero byte
			continue
		}

		reader.buffer.Peek(config.BlockSize)
		break
	}

	entry := Entry{}
	entry.deserialize(reader.buffer)

	return entry, nil
}

func (writer *SSTableWriter) spaceAvailableInBlock(size int) bool {
	return (config.BlockSize - writer.currentBlockLen) >= size
}

func (writer *SSTableWriter) padBlock() {
	padding := config.BlockSize - writer.currentBlockLen
	// log.Printf("About to write padding %v", padding)

	_, err := writer.buffer.Write(make([]byte, padding))
	if err != nil {
		panic(err)
	}

	writer.currentBlockLen = 0
	writer.currentBlock++
	log.Printf("Padded and new block count is %d", writer.currentBlock)
}

func (writer *SSTableWriter) writeSingleEntry(entry *[]byte, size int) error {
	if size > config.BlockSize {
		//Will never fit
		return errors.New("entry larger than max block size")
	}

	writer.currentBlockLen += size
	_, err := writer.buffer.Write(*entry)
	panicIfErr(err)
	writer.buffer.Flush()
	return nil
}

func (writer *SSTableWriter) writeFromMemtable(memtable *Memtable) error {
	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(Entry)
		size, serialized_entry := entry.serialize()
		if !writer.spaceAvailableInBlock(size) {
			writer.padBlock()
		}
		if writer.currentBlock >= config.SSTableBlockCount {
			fileName := fileManager.getNextFilename()
			currentWriter = newSSTableWriterFromPath(fmt.Sprintf("%v/%v/%v", config.DataDirectory, "0", fileName))
			fileManager.addFileToLedger(fileName, 0)
		}
		err := writer.writeSingleEntry(&serialized_entry, size)
		if err != nil {
			return err
		}
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
