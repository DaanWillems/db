package storage

import (
	"bufio"
	"bytes"
	"os"
)

func getSmallestKey(initial_min []byte, readers map[string]*SSTableReader) (string, *SSTableReader, []byte) {
	min := initial_min
	var smallestReader *SSTableReader
	var smallestPath string

	//Get reader with smallest key
	for path, reader := range readers {
		id, _ := reader.peekNextId()
		if bytes.Compare(id, min) == -1 {
			min = id
			smallestReader = reader
			smallestPath = path
		}
	}

	return smallestPath, smallestReader, min
}

func compactNSSTables(input_paths []string, output_path string) {
	fd_writer, err := os.OpenFile(output_path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 644)
	panicIfErr(err)
	writer_buffer := bufio.NewWriter(fd_writer)
	writer := newSSTableWriter(writer_buffer)

	//Create readers
	inputs := map[string]*SSTableReader{}

	var min []byte
	var currentPath string
	var currentReader *SSTableReader

	for _, path := range input_paths {
		fd, err := os.Open(path)
		logFatal(err)
		reader := newSSTableReader(bufio.NewReader(fd))
		inputs[path] = &reader
		id, _ := inputs[path].peekNextId()
		if bytes.Compare(id, min) == 1 {
			min = id
		}
	}

	for {
		currentPath, currentReader, min = getSmallestKey(min, inputs)

		entry, err := currentReader.readNextEntry()
		if checkEOF(err) {
			//Remove from map
			delete(inputs, currentPath)
		}

		writer.writeSingleEntry(&entry)
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
