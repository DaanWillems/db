package storage

import (
	"fmt"
	"log"
	"os"
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
var config Config

func InitializeStorageEngine(cfg Config) {
	config = cfg
	memtable = newMemtable()
	initFileManager(cfg.DataDirectory)
	replayWal(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	openWAL(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
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

		var byteSize int64
		byteSize = 0
		//Check if we should compact
		for _, path := range fileManager.getDataIndex()[0] { //Check level 0
			file, err := os.Stat(path)
			if err != nil {
				return err
			}

			byteSize += file.Size()
		}

		if byteSize > int64(config.Level0CompactionTriggerSize) {
			log.Printf("Should compact because size is %v\n", byteSize)

			readers := []*SSTableReader{}
			for _, path := range fileManager.getDataIndex()[0] { //Get L0 files
				reader := newSSTableReaderFromPath(path)
				readers = append(readers, &reader)
			}

			//Find overlapping L1 files

			paths, _ := compactNSSTables(readers, 1)
			log.Println(paths)
			for _, path := range fileManager.getDataIndex()[0] { //Get L0 files
				os.RemoveAll(path) //TODO: Do this properly
			}
		} else {
			log.Printf("Should not compact because size is %v\n", byteSize)
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
