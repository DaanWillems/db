package storage

import (
	"bufio"
	"errors"
	"strings"
)

type SSTableWriter struct {
	buffer          *bufio.Writer
	currentBlockLen int //The length of the current block we're writing to
	currentBlock    int //The current block we're writing to
	path            string
	fileName        string
}

func newSSTableWriterFromPath(path string) (SSTableWriter, error) {
	fd, err := fileManager.openWriteFile(path)
	if err != nil {
		return SSTableWriter{}, err
	}
	path_split := strings.Split(path, "/")
	return SSTableWriter{
		buffer:          bufio.NewWriter(fd),
		currentBlockLen: 0,
		path:            path,
		fileName:        path_split[len(path_split)-1],
	}, nil
}

func (writer *SSTableWriter) spaceAvailableInCurrentBlock(size int) bool {
	return (config.BlockSize - writer.currentBlockLen) >= size
}

func (writer *SSTableWriter) padCurrentBlock() {
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

	if !writer.spaceAvailableInCurrentBlock(len(*entry)) {
		writer.padCurrentBlock()
	}

	writer.currentBlockLen += len(*entry)
	_, err := writer.buffer.Write(*entry)
	panicIfErr(err)
	writer.buffer.Flush()
	return nil
}

func writeFromMemtable(memtable *Memtable) error {
	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(Entry)
		_, serialized_entry := entry.serialize()
		write(&serialized_entry)
	}
	return nil
}
