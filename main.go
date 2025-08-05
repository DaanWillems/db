package main

import (
	storage "db/storage"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
)

func main() {
	log.Println("Starting...")
	f, _ := os.Create("profile")
	pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	storage.InitializeStorageEngine(storage.Config{
		MemtableSize:  500,
		DataDirectory: "data",
	})

	for i := range 2500 {
		storage.Insert(i, []string{fmt.Sprintf("%v", i)})
	}

	storage.Compact()
	storage.Close()
}
