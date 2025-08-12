package main

import (
	storage "db/storage"
	"fmt"
	"log"
)

func main() {
	log.Println("Starting...")

	storage.InitializeStorageEngine(storage.Config{
		MemtableFlushSize:           800,
		DataDirectory:               "./data",
		BlockSize:                   200,
		SSTableBlockCount:           10,
		Level0CompactionTriggerSize: 6298, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := 0; i < 600; i++ {
		log.Printf("Inserting.. %v", i)
		storage.Insert(storage.IntToBytes(i), storage.IntToBytes(i))
	}

	storage.Insert(storage.IntToBytes(20), storage.IntToBytes(1))
	storage.Flush()
	storage.LevelledCompact()
	result, _ := storage.Query(storage.IntToBytes(20))
	fmt.Printf("%08b\n", result)

	storage.Close()
}
