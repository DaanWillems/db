package storage

import (
	"bytes"
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

func compact() {
	log.Println("Compacting L0")
	readers := []*SSTableIterator{}
	index := fileManager.getDataIndex()

	L0minID, _ := newSSTableScannerFromPath(index[0][0]).getFirstID()
	L0maxID, _ := newSSTableScannerFromPath(index[0][len(index[0])-1]).getLastID()

	for _, path := range index[0] { //Get L0 files
		reader := NewSSTableIteratorFromPath(path)
		readers = append(readers, reader)
	}

	for _, path := range index[1] { //Get L1 files
		scanner := newSSTableScannerFromPath(path)
		minID, _ := scanner.getFirstID()
		maxID, _ := scanner.getLastID()

		if bytes.Compare(maxID, L0maxID) == -1 || bytes.Compare(minID, L0minID) == 1 {
			reader := NewSSTableIteratorFromPath(path)
			readers = append(readers, reader)
		}
	}

	paths, _ := compactNSSTables(readers, 1)
	log.Printf("I have compacted the following files: %v Into: %v", readers, paths)
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
			compact()
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
			scanner := newSSTableScannerFromPath(path)
			entry, err := scanner.scan(id)
			if err != nil {
				log.Fatalf("Error iterating over entries: %v", err)
			}
			if entry != nil {
				return entry.value, nil
			}
		}
	}
	return nil, nil
}
