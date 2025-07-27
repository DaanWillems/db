package database

import (
	"bufio"
	"os"
)

type Config struct {
	MemtableSize int //Threshold of entries before flushing to disk
}

var memtable Memtable
var config Config

func InitializeDatabase(cfg Config) {
	memtable = NewMemtable()
	OpenWAL("wal.txt")
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
		table, err := CreateSSTableFromMemtable(&memtable, 10)
		if err != nil {
			panic(err)
		}

		table.Flush("./test.db")

		memtable = NewMemtable() // Reset memtable after flushing
		ResetWAL()
	}
}

func Query(id int) ([]byte, error) {
	//First check in the memtable
	entry := memtable.Get([]byte{byte(id)})

	if entry != nil {
		return entry.values[0], nil
	}

	//For now we only have the capability of writing and reading a single sstable from disk
	fd, err := os.Open("./test.db")

	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(fd) // creates a new reader

	entry, err = SearchInSSTable(reader, []byte{byte(id)})

	if err != nil {
		panic(err)
	}

	return entry.values[0], nil
}
