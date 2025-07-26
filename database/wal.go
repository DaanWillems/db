package database

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

var wal_file *os.File
var wal_path string

func OpenWAL(path string) {
	var err error
	wal_path = path
	wal_file, err = os.Create(path)
	if err != nil {
		panic(err)
	}
}

func WriteEntryToWal(entry MemtableEntry) {
	len, content := entry.Serialize()
	wal_file.Write([]byte{byte(len)})
	wal_file.Write(content)
	wal_file.Sync()
}

func ReplayWal() {
	fd, err := os.Open(wal_path)

	if err != nil {
		panic(err)
	}

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
		entry.Deserialize(content)

		var str_result string

		for _, v := range entry.values {
			str_result += string(v)
		}

		fmt.Printf("%v %v\n", entry.id, str_result)
	}
}
