package storage

import (
	"fmt"
	"log"
)

type Config struct {
	MemtableFlushSize           int //Threshold of entries before flushing to disk
	DataDirectory               string
	BlockSize                   int
	SSTableBlockCount           int
	Level0CompactionTriggerSize int //Maximum size before compaction is triggered in L0
	CompactionFactor            int //Each layer about L0 has a max size that is the Level0CompactionTriggerSize * CompactionFactor
	CompactionLevels            int
}

var memtable Memtable
var currentWriter SSTableWriter
var config Config

func InitializeStorageEngine(cfg Config) {
	config = cfg
	memtable = newMemtable()
	initFileManager(cfg.DataDirectory)
	replayWal(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	openWAL(fmt.Sprintf("./%v/wal", cfg.DataDirectory))

	fileName := fileManager.getNextFilename()
	currentWriter = newSSTableWriterFromPath(fmt.Sprintf("%v/%v/%v", config.DataDirectory, "0", fileName))
	fileManager.addFileToLedger(fileName, 0)
}

func Close() {
	fileManager.close()
}

func Insert(id []byte, value []byte) error {
	entry := Entry{
		id:      id,
		value:   value,
		deleted: false,
	}

	writeEntryToWal(entry)
	memtable.insert(entry)

	if memtable.totalByteSize >= config.MemtableFlushSize {
		fileManager.storeMemtable(&memtable)
		memtable = newMemtable() // Reset memtable after flushing
		resetWAL()               //Discard the WAL

		if shouldCompactL0() {
			log.Println("Compacting L0")
			readers := []*SSTableReader{}
			for _, path := range fileManager.getDataIndex()[0] { //Get L0 files
				reader := newSSTableReaderFromPath(path)
				readers = append(readers, &reader)
			}

			lastId, err := readers[len(readers)-1].getLastId()
			if err != nil {
				log.Println(err)
				return nil
			}
			log.Printf("Last ID: %v\n", lastId)

			paths, _ := compactNSSTables(readers, 1)
			log.Println(paths)
		}
	}

	return nil
}

func Query(id []byte) ([]byte, error) {
	//First check in the memtable
	entry := memtable.Get(id)

	if entry != nil {
		return entry.value, nil
	}

	for index, paths := range fileManager.getDataIndex() {
		log.Printf("Search level %v", index)
		for _, path := range paths {
			reader := newSSTableReaderFromPath(path)
			entry, err := reader.scan(id)

			if err != nil {
				panic(err)
			}

			if entry == nil {
				continue
			}

			return entry.value, nil
		}
	}

	return nil, nil
}
