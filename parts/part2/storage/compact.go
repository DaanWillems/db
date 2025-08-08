package storage

import (
	"bytes"
)

func getNextEntry(readers []*SSTableReader) (*Entry, []*SSTableReader) {
	var min []byte
	outputReaders := []*SSTableReader{}
	emptyReaders := []*SSTableReader{}

	//Get reader with smallest key
	//Its assumed that readers are ordered oldest to newest
	for _, reader := range readers {
		id, err := reader.peekNextId()
		if checkEOF(err) {
			emptyReaders = append(emptyReaders, reader)
			continue
		}

		if min == nil {
			min = id
			outputReaders = append(outputReaders, reader)
		} else if bytes.Compare(id, min) == -1 {
			min = id
			outputReaders := []*SSTableReader{}
			outputReaders = append(outputReaders, reader)
		} else if bytes.Equal(id, min) {
			outputReaders = append(outputReaders, reader)
		}
	}

	var entry Entry

	for _, reader := range outputReaders {
		entry, _ = reader.readNextEntry() //use the latest (most recent) newest entry
	}

	return &entry, emptyReaders
}

func compactNSSTables(inputs []*SSTableReader, output *SSTableWriter) error {
	for {
		entry, emptyReaders := getNextEntry(inputs)
		output.writeSingleEntry(entry)

		for _, emptyReader := range emptyReaders {
			for index, reader := range inputs {
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
