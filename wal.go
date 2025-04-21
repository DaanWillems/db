package main

import "os"

type WAL struct {
	path string
}

func OpenWAL(path string) {
	_, err := os.OpenFile("wal.txt", os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
}
