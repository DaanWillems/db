package storage

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := newMemtable()

	for id := range 3000 {
		memtable.insertRaw(intToBytes(id), intToBytes(id))
	}
	buffer := bytes.Buffer{}
	writer := newSSTableWriter(bufio.NewWriter(&buffer))
	writer.writeFromMemtable(&memtable)

	reader := bufio.NewReaderSize(&buffer, 1024*1024)

	for id := range 3000 {
		result, _ := scanSSTable(reader, intToBytes(id))

		entry := Entry{
			intToBytes(id),
			intToBytes(id),
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
			return
		}
	}
}
