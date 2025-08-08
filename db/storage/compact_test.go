package storage

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

// Combine two indentical SSTables into 1
func TestFullCompaction(t *testing.T) {
	os.RemoveAll("./tmp")
	err := os.Mkdir("tmp", 0700)

	InitializeStorageEngine(Config{
		MemtableSize:  50,
		DataDirectory: "./tmp",
		BlockSize:     100,
	})
	panicIfErr(err)

	for id := range 50 {
		Insert(id, IntToBytes(id))
	}

	for id := range 50 {
		Insert(id, IntToBytes(id))
	}

	index := fileManager.getDataIndex()

	if len(index) < 2 {
		return
	}

	r1 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[0]))
	r2 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[1]))
	w1 := newSSTableWriterFromPath("./tmp/output")

	compactNSSTables([]*SSTableReader{&r1, &r2}, &w1)

	reader := newSSTableReaderFromPath("./tmp/output")

	for id := range 50 {
		result, _ := reader.scan(IntToBytes(id))

		entry := Entry{
			IntToBytes(id),
			IntToBytes(id),
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
		}
	}
	count := reader.count()
	if count != 50 {
		t.Errorf("Entry count does not match, expected 50 got %v", count)
	}
	os.RemoveAll("./tmp")
}

func TestUpdateCompaction(t *testing.T) {
	os.RemoveAll("./tmp")
	err := os.Mkdir("tmp", 0700)

	InitializeStorageEngine(Config{
		MemtableSize:  50,
		DataDirectory: "./tmp",
		BlockSize:     100,
	})
	panicIfErr(err)

	for id := range 50 {
		Insert(id, IntToBytes(1))
	}

	for id := range 50 {
		Insert(id, IntToBytes(5))
	}

	index := fileManager.getDataIndex()

	if len(index) < 2 {
		return
	}

	r1 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[0]))
	r2 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[1]))
	w1 := newSSTableWriterFromPath("./tmp/output")

	compactNSSTables([]*SSTableReader{&r1, &r2}, &w1)

	reader := newSSTableReaderFromPath("./tmp/output")

	result, _ := reader.scan(IntToBytes(2))

	entry := Entry{
		IntToBytes(2),
		IntToBytes(5),
		false,
	}

	if !reflect.DeepEqual(&entry, result) {
		t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
	}

	count := reader.count()
	if count != 50 {
		t.Errorf("Entry count does not match, expected 50 got %v", count)
	}
	os.RemoveAll("./tmp")
}

// Compact two fully disjoint SSTables
func TestNoCompaction(t *testing.T) {
	os.RemoveAll("./tmp")
	err := os.Mkdir("tmp", 0700)

	InitializeStorageEngine(Config{
		MemtableSize:  50,
		DataDirectory: "./tmp",
		BlockSize:     100,
	})
	panicIfErr(err)

	for id := range 100 {
		Insert(id, IntToBytes(id))
	}

	index := fileManager.getDataIndex()

	if len(index) < 2 {
		return
	}

	r1 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[0]))
	r2 := newSSTableReaderFromPath(fmt.Sprintf("./tmp/%v", index[1]))
	w1 := newSSTableWriterFromPath("./tmp/output")

	compactNSSTables([]*SSTableReader{&r1, &r2}, &w1)

	reader := newSSTableReaderFromPath("./tmp/output")

	for id := range 100 {
		result, _ := reader.scan(IntToBytes(id))

		entry := Entry{
			IntToBytes(id),
			IntToBytes(id),
			false,
		}

		if !reflect.DeepEqual(&entry, result) {
			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
		}
	}
	count := reader.count()
	if count != 100 {
		t.Errorf("Entry count does not match, expected 100 got %v", count)
	}
	os.RemoveAll("./tmp")
}
