package storage

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"testing"
)

func TestSSTable(t *testing.T) {
	os.RemoveAll("./tmp")
	os.Mkdir("./tmp", 0744)
	config = Config{
		BlockSize: 500,
	}

	memtable := newMemtable()

	for id := range 50 {
		memtable.insertRaw(IntToBytes(id), IntToBytes(id))
	}
	fd, _ := os.OpenFile("./tmp/data.sst", os.O_RDWR|os.O_CREATE, 0777)
	writer := SSTableWriter{
		buffer:          bufio.NewWriter(fd),
		currentBlockLen: 0,
		path:            "./tmp/data.sst",
	}

	for e := memtable.entries.Front(); e != nil; e = e.Next() {
		entry := e.Value.(Entry)
		_, serialized_entry := entry.serialize()
		writer.writeSingleEntry(&serialized_entry)
	}

	fd, err := os.Open("./tmp/data.sst")
	if err != nil {
		log.Fatal(err)
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		log.Fatal(err)
	}
	reader := SSTableReader{
		fileSize: fileInfo.Size(),
		file:     fd,
	}
	it := NewSSTableIterator(&reader)
	for id := range 50 {
		it.Next()
		if !bytes.Equal(it.Entry().value, IntToBytes(id)) {
			t.Error("Value does not match")
		}
	}
}

// func TestSSTableReuse(t *testing.T) {
// 	config = Config{
// 		BlockSize: 100,
// 	}

// 	memtable := newMemtable()

// 	for id := range 3000 {
// 		memtable.insertRaw(IntToBytes(id), IntToBytes(id))
// 	}

// 	buffer := bytes.Buffer{}
// 	writer := newSSTableWriter(bufio.NewWriter(&buffer))
// 	writer.writeFromMemtable(&memtable)

// 	reader := newSSTableReader(&buffer)
// 	result, _ := reader.scan(IntToBytes(5))

// 	if !reflect.DeepEqual(result.id, IntToBytes(5)) {
// 		t.Errorf("ID does not match")
// 	}

// 	result, _ = reader.scan(IntToBytes(2))
// 	if result == nil {
// 		t.Errorf("Result is nil")
// 		return
// 	}
// 	if !reflect.DeepEqual(result.id, IntToBytes(2)) {
// 		t.Errorf("ID does not mach")
// 	}

// }
