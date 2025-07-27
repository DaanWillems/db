package main

import (
	"db/database"
	"fmt"
)

func main() {
	database.InitializeDatabase(database.Config{
		MemtableSize: 4,
	})

	// database.Insert(1, []string{"a"})
	// database.Insert(2, []string{"b"})
	// database.Insert(2, []string{"c"})
	// database.Insert(4, []string{"d"})
	// database.Insert(5, []string{"e"})
	// database.Insert(6, []string{"f"})

	result, err := database.Query(1)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(result))
}
