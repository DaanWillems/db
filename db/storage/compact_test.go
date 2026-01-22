package storage

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

// Combine two indentical SSTables into 1
// func TestFullCompaction(t *testing.T) {
// 	os.RemoveAll("./tmp")
// 	err := os.Mkdir("tmp", 0700)

// 	InitializeStorageEngine(Config{
// 		MemtableFlushSize: 400,
// 		DataDirectory:     "./tmp",
// 		BlockSize:         11,
// 		CompactionLevels:  5,
// 		SSTableBlockCount: 49,
// 	})
// 	panicIfErr(err)

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(id))
// 	}

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(id))
// 	}

// 	index := fileManager.getDataIndex()

// 	if len(index[0]) < 2 {
// 		return
// 	}

// 	r1 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][0]))
// 	r2 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][1]))

// 	paths, _ := compactNSSTables([]*SSTableIterator{r1, r2}, 0)

// 	it := NewSSTableIteratorFromPath(paths[0])

// 	count := 0
// 	for it.Next() {
// 		count++
// 	}

// 	if count != 50 {
// 		t.Errorf("Count is incorrect. Expected %v, got %v", 50, count)
// 	}

// 	reader := newSSTableScannerFromPath(paths[0])

// 	for id := range 50 {
// 		result, _ := reader.scan(IntToBytes(id))

// 		entry := Entry{
// 			IntToBytes(id),
// 			IntToBytes(id),
// 			false,
// 		}

// 		if !reflect.DeepEqual(&entry, result) {
// 			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
// 		}
// 	}

// 	os.RemoveAll("./tmp")
// }

// func TestTripleCompaction(t *testing.T) {
// 	os.RemoveAll("./tmp")
// 	err := os.Mkdir("tmp", 0700)

// 	InitializeStorageEngine(Config{
// 		MemtableFlushSize: 400,
// 		DataDirectory:     "./tmp",
// 		BlockSize:         11,
// 		CompactionLevels:  5,
// 		SSTableBlockCount: 49,
// 	})
// 	panicIfErr(err)

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(id))
// 	}

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(id))
// 	}

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(id))
// 	}

// 	index := fileManager.getDataIndex()

// 	if len(index) < 2 {
// 		return
// 	}

// 	r1 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][0]))
// 	r2 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][1]))
// 	r3 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][2]))

// 	paths, _ := compactNSSTables([]*SSTableIterator{r1, r2, r3}, 0)

// 	it := NewSSTableIteratorFromPath(paths[0])

// 	count := 0
// 	for it.Next() {
// 		count++
// 	}

// 	if count != 50 {
// 		t.Errorf("Count is incorrect. Expected %v, got %v", 50, count)
// 	}

// 	reader := newSSTableScannerFromPath(paths[0])

// 	for id := range 50 {
// 		result, _ := reader.scan(IntToBytes(id))

// 		entry := Entry{
// 			IntToBytes(id),
// 			IntToBytes(id),
// 			false,
// 		}

// 		if !reflect.DeepEqual(&entry, result) {
// 			t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
// 		}
// 	}
// 	os.RemoveAll("./tmp")
// }

// func TestUpdateCompaction(t *testing.T) {
// 	os.RemoveAll("./tmp")
// 	err := os.Mkdir("tmp", 0700)

// 	InitializeStorageEngine(Config{
// 		MemtableFlushSize: 400,
// 		DataDirectory:     "./tmp",
// 		BlockSize:         11,
// 		CompactionLevels:  5,
// 		SSTableBlockCount: 50,
// 	})
// 	panicIfErr(err)

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(1))
// 	}

// 	for id := range 50 {
// 		Insert(IntToBytes(id), IntToBytes(5))
// 	}

// 	index := fileManager.getDataIndex()

// 	if len(index[0]) < 2 {
// 		return
// 	}

// 	r1 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][0]))
// 	r2 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][1]))

// 	paths, _ := compactNSSTables([]*SSTableIterator{r1, r2}, 0)

// 	it := NewSSTableIteratorFromPath(paths[0])

// 	count := 0
// 	for it.Next() {
// 		count++
// 	}

// 	if count != 50 {
// 		t.Errorf("Count is incorrect. Expected %v, got %v", 50, count)
// 	}

// 	reader := newSSTableScannerFromPath(paths[0])
// 	result, _ := reader.scan(IntToBytes(2))

// 	entry := Entry{
// 		IntToBytes(2),
// 		IntToBytes(5),
// 		false,
// 	}

// 	if !reflect.DeepEqual(&entry, result) {
// 		t.Errorf("Result does not match query. \nExpected: \n%v\n Got:\n %v", entry, result)
// 	}

// 	os.RemoveAll("./tmp")
// }

func TestNoCompaction(t *testing.T) {
	os.RemoveAll("./tmp")
	err := os.Mkdir("tmp", 0700)

	InitializeStorageEngine(Config{
		MemtableFlushSize: 400,
		DataDirectory:     "./tmp",
		BlockSize:         11,
		CompactionLevels:  5,
		SSTableBlockCount: 50,
	})
	panicIfErr(err)

	for id := range 100 {
		Insert(IntToBytes(id), IntToBytes(id))
	}

	index := fileManager.getDataIndex()

	if len(index[0]) < 2 {
		return
	}

	r1 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][0]))
	r2 := NewSSTableIteratorFromPath(fmt.Sprintf("%v", index[0][1]))

	paths, _ := compactNSSTables([]*SSTableIterator{r1, r2}, 0)

	it := NewSSTableIteratorFromPath(paths[0])

	count := 0
	for it.Next() {
		count++
	}

	if count != 100 {
		t.Errorf("Count is incorrect. Expected %v, got %v", 100, count)
	}

	reader := newSSTableScannerFromPath(paths[0])

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

	os.RemoveAll("./tmp")
}
