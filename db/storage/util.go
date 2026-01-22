package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
)

func rangeIsOverlapping(min0 []byte, max0 []byte, min1 []byte, max1 []byte) bool {
	if (bytes.Compare(min0, min1) == 1 && bytes.Compare(min0, max1) == -1) ||
		(bytes.Compare(min1, min0) == 1 && bytes.Compare(min1, max0) == -1) ||
		(bytes.Equal(min0, min1) || bytes.Equal(max0, max1)) {
		return true
	}
	return false
}

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
