package main

import (
	"fmt"
)

var memtable Memtable

func main() {
	init_db()

	for i := range 8 {
		insert(i, []string{"aa"})
	}

	table, err := CreateSSTableFromMetable(&memtable, 10)

	fmt.Printf("%v", table.Blocks)
	fmt.Println("")

	if err != nil {
		fmt.Printf("Error: %s", err)
		fmt.Println("")
		return
	}

	table.Flush("./test.db")
	entry, err := SearchInSSTable("./test.db", []byte{byte(2)})

	if err != nil {
		fmt.Printf("Error: %s", err)
		fmt.Println("")
		return
	}
	fmt.Printf("%v", entry)
	fmt.Println("")
}

func init_db() {
	memtable = NewMemtable()
}

func insert(id int, values []string) {
	byteValues := [][]byte{}
	for _, v := range values {
		byteValues = append(byteValues, []byte(v))
	}
	memtable.Insert([]byte{byte(id)}, byteValues)
}
