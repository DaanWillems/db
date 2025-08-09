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
		MemtableFlushSize:           500000,
		DataDirectory:               "data",
		BlockSize:                   10000,
		Level0CompactionTriggerSize: 5225349, //In bytes
		CompactionFactor:            10,
		CompactionLevels:            5,
	})

	for i := range 10000 {
		storage.Insert(storage.IntToBytes(i), make([]byte, 500))
	}

	result, _ := storage.Query(storage.IntToBytes(21))
	fmt.Printf("%08b\n", result)

	//storage.Compact()
	storage.Close()
}
