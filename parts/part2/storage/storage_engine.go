package storage

import (
	"fmt"
)

type Config struct {
	MemtableSize  int //Threshold of entries before flushing to disk
	DataDirectory string
	BlockSize     int
}

var memtable Memtable
var config Config

func InitializeStorageEngine(cfg Config) {
	memtable = newMemtable()
	initFileManager(fmt.Sprintf("./%v/ledger", cfg.DataDirectory))
	replayWal(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	openWAL(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	config = cfg
}

func Close() {
	closeWAL()
	fileManager.close()
}

func Compact() {
	index := fileManager.getDataIndex()

	if len(index) < 2 {
		return
	}

	r1 := newSSTableReaderFromPath(fmt.Sprintf("./data/%v", index[0]))
	r2 := newSSTableReaderFromPath(fmt.Sprintf("./data/%v", index[1]))
	w1 := newSSTableWriterFromPath("test")

	compactNSSTables([]*SSTableReader{&r1, &r2}, &w1)
}

func Insert(id int, value []byte) {
	entry := Entry{
		id:      IntToBytes(id),
		value:   value,
		deleted: false,
	}

	writeEntryToWal(entry)
	memtable.insert(entry)

	if memtable.entries.Len() >= config.MemtableSize {
		fileManager.writeDataFile(&memtable)
		memtable = newMemtable() // Reset memtable after flushing
		resetWAL()               //Discard the WAL
	}
}

func Query(id []byte) ([]byte, error) {
	//First check in the memtable
	entry := memtable.Get(id)

	if entry != nil {
		return entry.value, nil
	}

	for _, path := range fileManager.getDataIndex() {
		reader := newSSTableReaderFromPath(fmt.Sprintf("./data/%v", path))
		entry, err := reader.scan(id)

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
