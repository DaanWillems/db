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

func compactNSSTables(inputs []*SSTableIterator, level int) ([]string, error) {
	state := []*SSTableIterator{}

	fileName := fmt.Sprintf("%v/tmp/%v", config.DataDirectory, fileManager.getNextFilename())
	fileNames := []string{fileName}
	output := newSSTableWriterFromPath(fileName) //TODO:Generate new file name

	for _, it := range inputs {
		if ok := it.Next(); !ok {
			continue
		}
		state = append(state, it)
	}

	for {
		var min []byte
		outputIt := []*SSTableIterator{}

		for _, it := range state {
			entry := it.Entry()
			if entry == nil {
				continue
			}

			if min == nil {
				min = it.Entry().id
				outputIt = append(outputIt, it)
			} else if bytes.Compare(it.Entry().id, min) == -1 {
				min = it.Entry().id
				outputIt = []*SSTableIterator{}
				outputIt = append(outputIt, it)
			} else if bytes.Equal(it.Entry().id, min) {
				outputIt = append(outputIt, it)
			}
		}

		_, serialized_entry := outputIt[len(outputIt)-1].Entry().serialize()

		if output.currentBlock >= config.SSTableBlockCount {
			fileName := fmt.Sprintf("%v/tmp/%v", config.DataDirectory, fileManager.getNextFilename())
			output = newSSTableWriterFromPath(fileName) //TODO:Generate new file name
			fileNames = append(fileNames, fileName)
		}

		output.writeSingleEntry(&serialized_entry)

		for _, it := range outputIt {
			if ok := it.Next(); !ok {
				for idx, it2 := range state {
					if it2 == it {
						state = append(state[:idx], state[idx+1:]...)
						break
					}
				}
			}
		}
		if len(state) == 0 {
			return []string{fileName}, nil
		}
		if len(state) == 1 {
			_, serialized_entry := state[0].Entry().serialize()
			output.writeSingleEntry(&serialized_entry)
			for state[0].Next() {
				_, serialized_entry := state[0].Entry().serialize()
				output.writeSingleEntry(&serialized_entry)
			}
			return fileNames, nil
		}
	}
}
