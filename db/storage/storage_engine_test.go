package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestStorageEngineCompaction(t *testing.T) {
	os.RemoveAll("./tmp")

	InitializeStorageEngine(Config{
		MemtableFlushSize:           100000000,
		DataDirectory:               "./tmp",
		BlockSize:                   200,
		SSTableBlockCount:           1000000,
		Level0CompactionTriggerSize: 6298, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := range 300 {
		Insert(IntToBytes(i), IntToBytes(i))
	}

	Flush()
	LevelledCompact()

	for i := 100; i < 200; i++ {
		Insert(IntToBytes(i), IntToBytes(2))
	}

	Flush()
	LevelledCompact()

	//Assert index is correct
	if len(fileManager.getDataIndex()[0]) != 0 {
		t.Error("Found more .sst files then expected in level 0.\n")
	}
	if len(fileManager.getDataIndex()[1]) != 1 {
		t.Error("Found more .sst files then expected in level 1.\n")
	}

	it := NewSSTableIteratorFromPath(fileManager.getDataIndex()[1][0])

	for i := range 100 {
		it.Next()
		if !bytes.Equal(it.Entry().value, IntToBytes(i)) {
			t.Errorf("Content of sstable is not correct. Expected %v for ID %v, but got %v", IntToBytes(i), it.Entry().id, it.Entry().value)
		}
	}

	for range 100 {
		it.Next()
		if !bytes.Equal(it.Entry().value, IntToBytes(2)) {
			t.Errorf("Content of sstable is not correct. Expected %v for ID %v, but got %v", IntToBytes(2), it.Entry().id, it.Entry().value)
		}
	}

	for i := 200; i < 300; i++ {
		it.Next()
		if !bytes.Equal(it.Entry().value, IntToBytes(i)) {
			t.Errorf("Content of sstable is not correct. Expected %v for ID %v, but got %v", IntToBytes(i), it.Entry().id, it.Entry().value)
		}
	}

	if it.Next() {
		t.Errorf("Unexpected amount of entries remaining")
	}
}

func TestStorageEngine(t *testing.T) {
	os.RemoveAll("./tmp")

	InitializeStorageEngine(Config{
		MemtableFlushSize:           100000000,
		DataDirectory:               "./tmp",
		BlockSize:                   200,
		SSTableBlockCount:           10,
		Level0CompactionTriggerSize: 6298, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := range 100 {
		Insert(IntToBytes(i), IntToBytes(i))
	}

	Flush()

	//Assert index is correct
	if len(fileManager.getDataIndex()[0]) != 1 {
		t.Error("Found more .sst files then expected.\n")
	}

	it := NewSSTableIteratorFromPath(fileManager.getDataIndex()[0][0])

	for i := range 100 {
		it.Next()
		if !bytes.Equal(it.Entry().value, IntToBytes(i)) {
			t.Errorf("Content of sstable is not correct. Expected %v for ID %v, but got %v", IntToBytes(i), it.Entry().id, it.Entry().value)
		}
	}
}
