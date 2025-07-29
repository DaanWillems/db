package storage

import (
	"bufio"
	"io"
	"os"
)

var wal_file *os.File
var wal_path string

func openWAL(path string) {
	var err error
	wal_path = path
	wal_file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
}

func resetWAL() {
	var err error
	wal_file.Close()
	wal_file, err = os.Create(wal_path)
	if err != nil {
		panic(err)
	}
}

func writeEntryToWal(entry MemtableEntry) {
	len, content := entry.serialize()
	wal_file.Write([]byte{byte(len)})
	wal_file.Write(content)
	wal_file.Sync()
}

func replayWal(_wal_path string) {
	fd, err := os.Open(_wal_path)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	}
	defer fd.Close()

	reader := bufio.NewReader(fd)

	for {
		entryLen := make([]byte, 1)
		_, err = reader.Read(entryLen)

		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		content := make([]byte, int(entryLen[0]))
		_, err = reader.Read(content)

		if err != nil {
			panic(err)
		}

		entry := &MemtableEntry{}
		entry.deserialize(content)

		memtable.insert(*entry)
	}
}
