package main

import (
	storage "db/storage"
)

func main() {
	storage.InitializeStorageEngine(storage.Config{
		MemtableFlushSize:           1000,
		DataDirectory:               "./data",
		BlockSize:                   500,
		SSTableBlockCount:           5000,
		Level0CompactionTriggerSize: 6298000, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := 0; i < 600; i++ {
		storage.Insert(storage.IntToBytes(i), storage.IntToBytes(i))
	}

	storage.Close()
}
