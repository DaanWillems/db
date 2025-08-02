package storage

import (
	"fmt"
)

type Config struct {
	MemtableSize int //Threshold of entries before flushing to disk
}

var memtable Memtable
var config Config

func InitializeStorageEngine(cfg Config) {
	memtable = newMemtable()
	loadFileLedger()
	replayWal("./data/wal")
	openWAL("./data/wal")
	compact()
	config = cfg
}

func compact() {
	index := getDataIndex()

	if len(index) < 2 {
		return
	}

	compactSSTables(fmt.Sprintf("./data/%v", index[0]), fmt.Sprintf("./data/%v", index[1]), "./test")
}

func Insert(id int, values []string) {
	byteValues := [][]byte{}
	for _, v := range values {
		byteValues = append(byteValues, []byte(v))
	}

	entry := MemtableEntry{
		id:      []byte{byte(id)},
		values:  byteValues,
		deleted: false,
	}

	writeEntryToWal(entry)
	memtable.insert(entry)

	if memtable.entries.Len() >= config.MemtableSize {
		writeDataFile(&memtable)

		memtable = newMemtable() // Reset memtable after flushing
		resetWAL()               //Discard the WAL
	}
}

func Query(id int) ([]byte, error) {
	//First check in the memtable
	entry := memtable.Get([]byte{byte(id)})

	if entry != nil {
		return entry.values[0], nil
	}

	for _, path := range getDataIndex() {
		entry, err := scanSSTable(fmt.Sprintf("./data/%v", path), []byte{byte(id)})

		if err != nil {
			panic(err)
		}

		if entry == nil {
			continue
		}

		return entry.values[0], nil
	}

	return nil, nil
}
