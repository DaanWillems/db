package main

import (
	storage "db/storage"
	"fmt"
)

func main() {
	storage.InitializeStorageEngine(storage.Config{
		MemtableSize: 10000,
	})

	// for i := range 50000 {
	// 	storage.Insert(i, []string{fmt.Sprintf("%v", i)})
	// }

	result, err := storage.Query(249)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(result))
}
