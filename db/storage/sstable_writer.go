package storage

import (
	"bufio"
	"errors"
	"fmt"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
	currentBlock    int //The current block we're writing to
	path            string
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

func (writer *SSTableWriter) spaceAvailableInBlock(size int) bool {
	return (config.BlockSize - writer.currentBlockLen) >= size
}

func (writer *SSTableWriter) padBlock() {
	padding := config.BlockSize - writer.currentBlockLen

	_, err := writer.buffer.Write(make([]byte, padding))
	if err != nil {
		panic(err)
	}

	writer.currentBlockLen = 0
	writer.currentBlock++
}

func (writer *SSTableWriter) writeSingleEntry(entry *[]byte) error {
	if len(*entry) > config.BlockSize {
		//Will never fit
		return errors.New("entry larger than max block size")
	}

	if !writer.spaceAvailableInBlock(len(*entry)) {
		writer.padBlock()
	}

	writer.currentBlockLen += len(*entry)
	_, err := writer.buffer.Write(*entry)
	panicIfErr(err)
	writer.buffer.Flush()
	return nil
}

func (writer *SSTableWriter) writeFromMemtable(memtable *Memtable) error {
	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(Entry)
		_, serialized_entry := entry.serialize()
		if writer.currentBlock >= config.SSTableBlockCount {
			fileName := fileManager.getNextFilename()
			currentWriter = newSSTableWriterFromPath(fmt.Sprintf("%v/%v/%v", config.DataDirectory, "0", fileName))
			fileManager.addFileToLedger(fileName, 0)
		}
		err := writer.writeSingleEntry(&serialized_entry)
		if err != nil {
			return err
		}
	}
	return nil
}
