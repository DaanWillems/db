package storage

import (
	"bytes"
	"fmt"
	"os"
)

func shouldCompactL0() bool {
	var byteSize int64
	byteSize = 0
	//Check if we should compact
	for _, path := range fileManager.getDataIndex()[0] { //Check level 0
		file, err := os.Stat(path)
		if err != nil {
			return false
		}

		byteSize += file.Size()
	}
	return byteSize > int64(config.Level0CompactionTriggerSize)
}

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

// TODO: Write to temp files during compaction, and copy over atomatically
// Returns a sorted list of paths to files
func compactNSSTables(inputs []*SSTableReader, level int) ([]string, error) {
	output := newSSTableWriterFromPath(fmt.Sprintf("%v/tmp/%v", config.DataDirectory, fileManager.getNextFilename())) //TODO:Generate new file name

	for {
		entry, emptyReaders := getNextEntry(inputs)
		size, serialized_entry := entry.serialize()
		output.writeSingleEntry(&serialized_entry, size)

		for _, emptyReader := range emptyReaders {
			for index, reader := range inputs {
				if reader == emptyReader {
					//Remove from map
					inputs = append(inputs[:index], inputs[index+1:]...)
				}
			}
		}

		if len(inputs) == 0 {
			return []string{output.path}, nil
		}
		if len(inputs) == 1 {
			for _, remainder := range inputs {
				for {
					entry, err := remainder.readNextEntry()
					if checkEOF(err) {
						return []string{output.path}, nil
					}
					if err != nil {
						return nil, err
					}
					size, serialized_entry := entry.serialize()
					output.writeSingleEntry(&serialized_entry, size)
				}
			}
		}
	}
}
