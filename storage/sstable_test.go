package storage

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := NewMemtable()

	for id := range 10 {
		memtable.insertRaw([]byte{byte(id)}, [][]byte{{byte(id)}, []byte("b")})
	}

	table, _ := createSSTableFromMemtable(&memtable, 10)

	tableBytes := table.bytes()

	reader := bufio.NewReader(bytes.NewReader(tableBytes))

	for id := range 10 {
		result, _ := searchInSSTable(reader, []byte{byte(id)})

		entry := MemtableEntry{
			[]byte{byte(id)},
			[][]byte{{byte(id)}, []byte("b")},
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
		}
	}
}
