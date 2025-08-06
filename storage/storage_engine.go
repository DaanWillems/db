package storage

import (
	"bufio"
	"fmt"
	"os"
)

type Config struct {
	MemtableSize  int //Threshold of entries before flushing to disk
	DataDirectory string
}

var memtable Memtable
var config Config

func InitializeStorageEngine(cfg Config) {
	memtable = newMemtable()
	loadFileLedger(fmt.Sprintf("./%v/ledger", cfg.DataDirectory))
	replayWal(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	openWAL(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	config = cfg
}

func Close() {
	closeWAL()
	closeLedger()
}

func Compact() {
	index := getDataIndex()

	if len(index) < 2 {
		return
	}

	fd1, err := os.Open(fmt.Sprintf("./data/%v", index[0]))
	panicIfErr(err)
	fd2, err := os.Open(fmt.Sprintf("./data/%v", index[1]))
	panicIfErr(err)
	fd3, err := os.OpenFile("test", os.O_CREATE|os.O_RDWR, 0644)
	panicIfErr(err)

	b1 := newSSTableReader(bufio.NewReader(fd1))
	b2 := newSSTableReader(bufio.NewReader(fd2))
	b3 := newSSTableWriter(bufio.NewWriter(fd3))

	compactNSSTables([]*SSTableReader{&b1, &b2}, &b3)
}

func Insert(id int, value []byte) {
	entry := Entry{
		id:      []byte{byte(id)},
		value:   value,
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
		return entry.value, nil
	}

	for _, path := range getDataIndex() {
		fd, err := os.Open(fmt.Sprintf("./data/%v", path))
		panicIfErr(err)

		entry, err := scanSSTable(bufio.NewReader((fd)), []byte{byte(id)})

		if err != nil {
			panic(err)
		}

		if entry == nil {
			continue
		}

		return entry.value, nil
	}

	return nil, nil
}
