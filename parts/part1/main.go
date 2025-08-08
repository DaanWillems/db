package main

import "log"

func main() {
	memtable := newMemtable()
	memtable.insert(Entry{
		id:    IntToBytes(1),
		value: IntToBytes(23),
	})

	result := memtable.get(IntToBytes(1))
	log.Printf("Result ID: %v, Value: %v\n", result.id, result.value)

	for i := range 100 {
		memtable.insert(Entry{
			id:    IntToBytes(i),
			value: IntToBytes(i),
		})
	}

	writer := newSSTableWriterFromPath("./data.db")
	writer.writeFromMemtable(&memtable)

	reader := newSSTableReaderFromPath("./data.db")
	result, err := reader.scan(IntToBytes(20))
	logFatal(err)

	log.Printf("Result ID: %v, Value: %v\n", result.id, result.value)
}
