package main

import (
	"fmt"
)

var memtable Memtable

func main() {
	init_db()

	for i := range 100 {
		insert(i, []string{"AppelBanaan", "B"})
	}

	table, err := CreateSSTableFromMetable(&memtable)

	fmt.Printf("%v", table.Blocks)
	fmt.Println("")

	if err != nil {
		fmt.Printf("Error: %s", err)
		fmt.Println("")
		return
	}

	table.Flush("./test.db")
	_, err = SearchInSSTable("./test.db", []byte{byte(102)})

	if err != nil {
			fmt.Printf("Error: %s", err)
			fmt.Println("")
			return
		}
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
