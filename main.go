package main

import (
	"bufio"
	"fmt"
	"os"
)

var memtable Memtable

func main() {
	//Startup core processes
	//Start write engine


	//Start compactor


	//Start REPL
	init_db()

	for i := range 8 {
		insert(i, []string{"aab"})
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
	fd, err := os.Open("./test.db")
	reader := bufio.NewReader(fd) // creates a new reader

	entry, err := SearchInSSTable(reader, []byte{byte(2)})

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
