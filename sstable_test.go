package main

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

var testMemTable Memtable

func TestSSTable(t *testing.T) {
	memtable = NewMemtable()

	for id := range 10 {
		memtable.Insert([]byte{byte(id)}, [][]byte{{byte(id)}, []byte("b")})
	}

	table, _ := CreateSSTableFromMetable(&memtable, 10)

	tableBytes := table.Bytes()

	reader := bufio.NewReader(bytes.NewReader(tableBytes))

	for id := range 10 {
		result, _ := SearchInSSTable(reader, []byte{byte(id)})

		entry := MemtableEntry{
			[]byte{byte(id)},
			[][]byte{{byte(id)}, []byte("b")},
			false,
		}

		if !reflect.DeepEqual(entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
		}
	}
}
