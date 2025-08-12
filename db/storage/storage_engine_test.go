package storage

import (
	"os"
	"testing"
)

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

	for i := 0; i < 100; i++ {
		Insert(IntToBytes(i), IntToBytes(1))
	}

	Flush()
}
