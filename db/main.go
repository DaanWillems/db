package main

import (
	storage "db/storage"
	"fmt"
	"log"
	"os"
)

func main() {
	log.Println("Starting...")
	os.RemoveAll("./data")
	os.Mkdir("./data", 0700)

	storage.InitializeStorageEngine(storage.Config{
		MemtableFlushSize:           500,
		DataDirectory:               "./data",
		BlockSize:                   200,
		SSTableBlockCount:           10,
		Level0CompactionTriggerSize: 5, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := range 40 {
		storage.Insert(storage.IntToBytes(i), make([]byte, 100))
	}

	result, _ := storage.Query(storage.IntToBytes(499))
	fmt.Printf("%08b\n", result)

	//storage.Compact()
	storage.Close()
}
