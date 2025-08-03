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
	defer pprof.StopCPUProfile()

	storage.InitializeStorageEngine(storage.Config{
		MemtableSize: 10000,
	})

	// for i := range 250000 {
	// 	storage.Insert(i, []string{fmt.Sprintf("%v", i)})
	// }

	result, err := storage.Query(1)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(result))

	storage.Close()
}
