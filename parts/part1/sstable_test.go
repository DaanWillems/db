package main

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := newMemtable()

	for id := range 3000 {
		memtable.insertRaw(IntToBytes(id), IntToBytes(id))
	}

	buffer := bytes.Buffer{}
	writer := newSSTableWriter(bufio.NewWriter(&buffer))
	writer.writeFromMemtable(&memtable)

	reader := newSSTableReader(&buffer)

	for id := range 3000 {
		result, _ := reader.scan(IntToBytes(id))

		entry := Entry{
			IntToBytes(id),
			IntToBytes(id),
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
			return
		}
	}
}

func TestSSTableReuse(t *testing.T) {
	memtable := newMemtable()

	for id := range 3000 {
		memtable.insertRaw(IntToBytes(id), IntToBytes(id))
	}

	buffer := bytes.Buffer{}
	writer := newSSTableWriter(bufio.NewWriter(&buffer))
	writer.writeFromMemtable(&memtable)

	reader := newSSTableReader(&buffer)
	result, _ := reader.scan(IntToBytes(5))

	if !reflect.DeepEqual(result.id, IntToBytes(5)) {
		t.Errorf("ID does not match")
	}

	result, _ = reader.scan(IntToBytes(2))
	if result == nil {
		t.Errorf("Result is nil")
		return
	}
	if !reflect.DeepEqual(result.id, IntToBytes(2)) {
		t.Errorf("ID does not mach")
	}

}
