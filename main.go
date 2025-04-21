package main

import (
	"fmt"
)

var memtable Memtable

func main() {
	init_db()
	insert(1, []string{"a"})
	insert(2, []string{"a"})
	insert(3, []string{"a"})
	insert(4, []string{"a"})

	table, err := CreateSSTableFromMetable(&memtable)
	if err != nil {
		fmt.Printf("Error: %s", err)
		fmt.Println("")
		return
	}

	fmt.Printf("Len: %d", len(*table.Blocks))
	fmt.Printf("%08b\n", *table.Blocks)
	fmt.Printf("%v", *table.Blocks)
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
