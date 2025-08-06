package storage

import (
	"log"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := newMemtable()

	for id := range 369 {
		log.Printf("Printing %d\n", id)
		memtable.insertRaw(intToBytes(id), intToBytes(id))
	}

	// buffer := bytes.Buffer{}
	// writer := newSSTableWriter(bufio.NewWriter(&buffer))
	writer := newSSTableWriterFromPath("abc")

	// writer.writeFromMemtable(&memtable)
	writer.writeFromMemtable(&memtable)

	// reader := bufio.NewReader(&buffer)

	reader2 := newSSTableReaderFromPath("abc")
	// result, _ := scanSSTable(reader, intToBytes(368))
	result, _ := scanSSTable(reader2.reader, intToBytes(368))

	entry := Entry{
		intToBytes(368),
		intToBytes(368),
		false,
	}

	if !reflect.DeepEqual(&entry, result) {
		t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
	}
}
