package storage

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := newMemtable()

	for id := range 10 {
		memtable.insertRaw([]byte{byte(id)}, [][]byte{{byte(id)}, []byte("b")})
	}

	buffer := bytes.Buffer{}
	writer := newSSTableWriter(bufio.NewWriter(&buffer))

	writer.writeFromMemtable(&memtable)

	reader := bufio.NewReader(&buffer)

	for id := range 10 {
		result, _ := scanSSTable(reader, []byte{byte(id)})

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
