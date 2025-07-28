package database

import (
	"bufio"
	"fmt"
	"os"
)

type Config struct {
	MemtableSize int //Threshold of entries before flushing to disk
}

var memtable Memtable
var config Config

func InitializeDatabase(cfg Config) {
	memtable = NewMemtable()
	LoadFileIndex()
	ReplayWal("./data/wal")
	OpenWAL("./data/wal")
	config = cfg
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

	WriteEntryToWal(entry)
	memtable.Insert(entry)

	if memtable.entries.Len() >= config.MemtableSize {
		table, err := CreateSSTableFromMemtable(&memtable, 100)
		if err != nil {
			panic(err)
		}

		WriteDataFile(table)

		memtable = NewMemtable() // Reset memtable after flushing
		ResetWAL()               //Discard the WAL
	}
}

func Query(id int) ([]byte, error) {
	//First check in the memtable
	entry := memtable.Get([]byte{byte(id)})

	if entry != nil {
		return entry.values[0], nil
	}

	for _, path := range getDataIndex() {
		fd, err := os.Open(fmt.Sprintf("./data/%v", path))

		if err != nil {
			panic(err)
		}

		reader := bufio.NewReader(fd)
		entry, err = SearchInSSTable(reader, []byte{byte(id)})

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
