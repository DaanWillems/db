package main

import (
	storage "db/storage"
)

func main() {
	storage.InitializeStorageEngine(storage.Config{
		MemtableSize: 2,
	})

	// for i := range 4 {
	// 	storage.Insert(i, []string{fmt.Sprintf("%v", i)})
	// }

	// result, err := storage.Query(80)

	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(result))
}
