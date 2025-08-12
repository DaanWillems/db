package storage

import (
	"bytes"
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
var currentWriter SSTableWriter
var config Config

func InitializeStorageEngine(cfg Config) {
	config = cfg
	memtable = newMemtable()
	initFileManager(cfg.DataDirectory)
	replayWal(fmt.Sprintf("./%v/wal", cfg.DataDirectory))
	openWAL(fmt.Sprintf("./%v/wal", cfg.DataDirectory))

	fileName := fileManager.getNextFilename()
	currentWriter = newSSTableWriterFromPath(fmt.Sprintf("%v/%v/%v", config.DataDirectory, "tmp", fileName))
}

func Close() {
	fileManager.close()
}

func LevelledCompact() {
	log.Println("Compacting L0")
	readers := []*SSTableIterator{}
	index := fileManager.getDataIndex()

	var L0minID []byte
	var L0maxID []byte

	for idx := range index[0] {
		minID, _ := newSSTableScannerFromPath(index[0][idx]).getFirstID()
		maxID, _ := newSSTableScannerFromPath(index[0][idx]).getLastID()
		if L0minID == nil || bytes.Compare(L0minID, minID) == 1 {
			L0minID = minID
		}
		if L0maxID == nil || bytes.Compare(L0maxID, maxID) == -1 {
			L0maxID = maxID
		}
	}

	for _, path := range index[0] { //Get L0 files
		reader := NewSSTableIteratorFromPath(path)
		readers = append(readers, reader)
	}

	for _, path := range index[1] { //Get L1 files
		scanner := newSSTableScannerFromPath(path)
		minID, _ := scanner.getFirstID()
		maxID, _ := scanner.getLastID()

		//TODO test this check..
		if rangeIsOverlapping(minID, maxID, L0minID, L0maxID) {
			reader := NewSSTableIteratorFromPath(path)
			readers = append([]*SSTableIterator{reader}, readers...)
		}
	}

	paths, _ := compactNSSTables(readers, 1)
	for _, path := range paths {
		log.Printf("Adding file to ledger level 1: %v\n", path)
		fileManager.addFileToLedger(path, 1)
	}

	log.Printf("I have compacted the following files into: %v", paths)
	for _, r := range readers {
		fileManager.deleteFileFromLedger(r.path, 0)
		fileManager.deleteFileFromLedger(r.path, 1)
		os.Remove(r.path)
		log.Printf("	- %v", r.path)
	}
}

func SwapWriter() {
	//Move file
	fileManager.addFileToLedger(currentWriter.path, 0)
	fileName := fileManager.getNextFilename()
	currentWriter = newSSTableWriterFromPath(fmt.Sprintf("%v/%v/%v", config.DataDirectory, "tmp", fileName))
}

func Flush() {
	log.Println("Flushing..")
	fileManager.storeMemtable(&memtable)
	memtable = newMemtable() // Reset memtable after flushing
	resetWAL()               //Discard the WAL
	SwapWriter()
}

func write(entry *[]byte) error {
	if currentWriter.currentBlock >= config.SSTableBlockCount {
		SwapWriter()
	}
	err := currentWriter.writeSingleEntry(entry)
	if err != nil {
		return err
	}
	return nil
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
		Flush()
		if shouldCompactL0() {
			LevelledCompact()
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
