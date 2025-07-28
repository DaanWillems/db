package main

import (
	"db/database"
	"fmt"
)

func main() {
	database.InitializeDatabase(database.Config{
		MemtableSize: 20,
	})

	for i := range 200 {
		database.Insert(i, []string{fmt.Sprintf("%v", i)})
	}

	result, err := database.Query(80)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(result))
}
