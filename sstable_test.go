package main

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

var testMemTable Memtable

func TestSSTable(t *testing.T) {
	memtable = NewMemtable()

	for id := range 10 {
		memtable.Insert([]byte{byte(id)}, [][]byte{[]byte{byte(id)}, []byte("b")})
	}

	table, _ := CreateSSTableFromMetable(&memtable, 10)

	tableBytes := table.Bytes()
	fmt.Printf("%v", tableBytes)

	reader := bufio.NewReader(bytes.NewReader(tableBytes))
	result, _ := SearchInSSTable(reader, []byte{byte(2)})

	entry := MemtableEntry{
		[]byte{byte(2)},
		[][]byte{{byte(2)}, []byte("b")},
		false,
	}

	if !reflect.DeepEqual(entry, result) {
		t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
	}
}
