package main

import (
	storage "db/storage"
	"fmt"
)

func main() {
	storage.InitializeStorageEngine(storage.Config{
		MemtableSize: 20,
	})

	// for i := range 200 {
	// 	storage.Insert(i, []string{fmt.Sprintf("%v", i)})
	// }

	result, err := storage.Query(80)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(result))
}
