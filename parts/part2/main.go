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
		MemtableSize:  500,
		DataDirectory: "data",
		BlockSize:     100,
	})

	for i := range 1000 {
		storage.Insert(i, storage.IntToBytes(i))
	}

	result, _ := storage.Query(storage.IntToBytes(21))
	fmt.Printf("%08b\n", result)

	//storage.Compact()
	storage.Close()
}
