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

	// for i := range 1000 {
	// 	storage.Insert(i, [][]byte{[]byte{byte(i)}})
	// }
	result, _ := storage.Query(999)
	fmt.Printf("%08b\n", 999)
	fmt.Printf("%08b\n", result)

	//storage.Compact()
	storage.Close()
}
