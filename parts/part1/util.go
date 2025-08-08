package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
)

func IntToBytes(i int) []byte {
	// Create a buffer
	buf := new(bytes.Buffer)

	// Write the integer to the buffer in BigEndian (you can also use LittleEndian)
	err := binary.Write(buf, binary.BigEndian, int32(i))
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}

	byteSlice := buf.Bytes()
	return byteSlice
}

func checkEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func logFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
