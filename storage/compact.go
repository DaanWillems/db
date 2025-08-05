package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

func getSmallestKey(readers []*SSTableReader) (*SSTableReader, []*SSTableReader) {
	var min []byte
	var smallestReader *SSTableReader
	emptyReaders := []*SSTableReader{}

	//Get reader with smallest key
	for path, reader := range readers {
		id, err := reader.peekNextId()
		if checkEOF(err) {
			emptyReaders = append(emptyReaders, reader)
			fmt.Printf("%v empty \n", path)
			continue
		}

		if min == nil {
			min = id
			smallestReader = reader
		} else if bytes.Compare(id, min) == -1 {
			min = id
			smallestReader = reader
		}
	}

	fmt.Printf("found min %v\n", min)
	return smallestReader, emptyReaders
}

func compactNSSTables(inputs []*SSTableReader, output *SSTableWriter) error {
	for {
		currentReader, emptyReaders := getSmallestKey(inputs)
		entry, err := currentReader.readNextEntry()
		if err != nil {
			return err
		}
		output.writeSingleEntry(&entry)

		for index, emptyReader := range emptyReaders {
			for _, reader := range inputs {
				if reader == emptyReader {
					//Remove from map
					inputs = append(inputs[:index], inputs[index+1:]...)
				}
			}
		}

		if len(inputs) == 0 {
			return nil
		}
		if len(inputs) == 1 {
			for _, remainder := range inputs {
				for {
					entry, err := remainder.readNextEntry()
					if checkEOF(err) {
						return nil
					}
					if err != nil {
						return err
					}
					output.writeSingleEntry(&entry)
				}
			}
		}
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
