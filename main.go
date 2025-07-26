package main

import (
	"db/database"
)

func main() {
	database.InitializeDatabase(database.Config{
		MemtableSize: 4,
	})

	database.Insert(1, []string{"a"})
	database.Insert(2, []string{"bcd"})
	database.Insert(3, []string{"c"})
	database.Insert(4, []string{"d"})

	database.Insert(5, []string{"e"})
	database.Insert(6, []string{"f"})
	database.Insert(7, []string{"g"})

	// result, err := database.Query(2)

	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(result))

	// result, err = database.Query(6)

	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(result))

	database.RestoreWal()
}
