package storage

import (
	"bufio"
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
		size, serialized_entry := entry.serialize()
		if !writer.spaceAvailableInBlock(size) {
			writer.padBlock()
		}
		writer.writeSingleEntry(&serialized_entry, size)
	}

	fd, err := os.Open("./tmp/data.sst")
	panicIfErr(err)
	fileInfo, err := fd.Stat()
	panicIfErr(err)
	reader := SSTableReader{
		fileSize: fileInfo.Size(),
		file:     fd,
	}
	it := NewSSTableIterator(&reader)
	for it.Next() {
		log.Printf("Entry: %v", it.Entry())
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
