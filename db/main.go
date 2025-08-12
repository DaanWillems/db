package main

import (
	storage "db/storage"
	"fmt"
	"log"
)

func main() {
	log.Println("Starting...")

	storage.InitializeStorageEngine(storage.Config{
		MemtableFlushSize:           500,
		DataDirectory:               "./data",
		BlockSize:                   200,
		SSTableBlockCount:           10,
		Level0CompactionTriggerSize: 6298, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := 300; i < 400; i++ {
		storage.Insert(storage.IntToBytes(i), storage.IntToBytes(5))
	}

	result, _ := storage.Query(storage.IntToBytes(300))
	fmt.Printf("%08b\n", result)

	storage.Close()
}
