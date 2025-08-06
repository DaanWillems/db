package storage

import (
	"log"
	"reflect"
	"testing"
)

func TestSSTable(t *testing.T) {
	memtable := newMemtable()

	for id := range 3000 {
		memtable.insertRaw(intToBytes(id), intToBytes(id))
	}

	fd, writer := newSSTableWriterFromPath("abc.db")
	writer.writeFromMemtable(&memtable)
	fd.Close()

	for id := range 3000 {
		reader2 := newSSTableReaderFromPath("abc.db")
		log.Printf("Starting %v\n", id)
		result, _ := scanSSTable(reader2.reader, intToBytes(id))

		entry := Entry{
			intToBytes(id),
			intToBytes(id),
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
			return
		}
		log.Printf("Finished %v\n", id)
	}
}
