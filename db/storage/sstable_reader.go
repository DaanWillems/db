package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type SSTableIterator struct {
	blockIterator *SSTableBlockIterator
	entryIterator *SSTableEntryIterator
	lastEntry     *Entry
}

func NewSSTableIterator(reader *SSTableReader) *SSTableIterator {
	return &SSTableIterator{
		blockIterator: NewSSTableBlockIterator(reader),
	}
}

func NewSSTableIteratorFromPath(path string) *SSTableIterator {
	return &SSTableIterator{
		blockIterator: NewSSTableBlockIteratorFromPath(path),
	}
}

func (it *SSTableIterator) Entry() *Entry {
	return it.lastEntry
}

func (it *SSTableIterator) Next() bool {
	if it.entryIterator == nil {
		if result := it.blockIterator.Next(); !result {
			return false
		}
		it.entryIterator = NewSSTableEntryIterator(it.blockIterator.Block())
	}

	foundEntry := it.entryIterator.Next()
	if !foundEntry {
		if result := it.blockIterator.Next(); !result {
			return false
		} else {
			it.entryIterator = NewSSTableEntryIterator(it.blockIterator.Block())
			if foundEntry := it.entryIterator.Next(); !foundEntry {
				return false
			}
		}
	}

	it.lastEntry = it.entryIterator.Entry()
	if it.lastEntry.id == nil {
		fmt.Printf("\n")
	}
	return true
}

type SSTableEntryIterator struct {
	entryIndex int
	lastEntry  *Entry
	error      error
	buffer     *bytes.Buffer
}

func NewSSTableEntryIterator(buffer *bytes.Buffer) *SSTableEntryIterator {
	return &SSTableEntryIterator{
		buffer: buffer,
	}
}

func (it *SSTableEntryIterator) Err() error {
	return it.error
}

func (it *SSTableEntryIterator) Entry() *Entry {
	return it.lastEntry
}

func (it *SSTableEntryIterator) Next() bool {
	entry := Entry{}
	err := entry.deserialize(it.buffer)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		it.error = err
		return false
	}
	it.lastEntry = &entry
	it.entryIndex += 1
	return true
}

type SSTableBlockIterator struct {
	blockIndex   int
	lastBlock    *bytes.Buffer
	currentEntry int
	error        error
	reader       *SSTableReader
}

func NewSSTableBlockIteratorFromPath(path string) *SSTableBlockIterator {
	reader := newSSTableReaderFromPath(path)
	return &SSTableBlockIterator{
		reader: &reader,
	}
}

func NewSSTableBlockIterator(reader *SSTableReader) *SSTableBlockIterator {
	return &SSTableBlockIterator{
		reader: reader,
	}
}

func (it *SSTableBlockIterator) Err() error {
	return it.error
}

func (it *SSTableBlockIterator) Block() *bytes.Buffer {
	return it.lastBlock
}
func (it *SSTableBlockIterator) Next() bool {
	block, err := it.reader.getBlock(it.blockIndex)
	if errors.Is(err, BlockOutOfBounds) {
		return false
	} else if err != nil {
		it.error = err
		return false
	}
	it.blockIndex++
	it.lastBlock = block
	return true
}

type SSTableReader struct {
	file     *os.File
	fileSize int64
}

func newSSTableReaderFromPath(path string) SSTableReader {
	fd, err := fileManager.openReadFile(path)
	panicIfErr(err)
	fileInfo, err := fd.Stat()
	panicIfErr(err)
	return SSTableReader{
		fileSize: fileInfo.Size(),
		file:     fd,
	}
}

func (reader *SSTableReader) getBlock(block int) (*bytes.Buffer, error) {
	offset := int64(block * config.BlockSize)
	readAmount := config.BlockSize

	if offset > reader.fileSize {
		return nil, BlockOutOfBounds
	}

	if int64(config.BlockSize) > (reader.fileSize - offset) {
		readAmount = int(reader.fileSize - offset)
	}

	blockBytes := make([]byte, readAmount)
	_, err := reader.file.ReadAt(blockBytes, offset)

	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(blockBytes)
	return buffer, nil
}

// func (reader *SSTableReader) getLastId() ([]byte, error) {
// 	//Calculate location for last block
// 	offset := int64(config.BlockSize * (config.SSTableBlockCount - 1))
// 	content := make([]byte, config.BlockSize)
// 	reader.file.ReadAt(content, offset)

// 	buffer := bufio.NewReader(bytes.NewBuffer(content))
// 	lastEntry := Entry{}
// 	for { //If the size is 0, the block is done:
// 		entry := Entry{}
// 		err := entry.deserialize(buffer)
// 		if errors.Is(err, io.ErrUnexpectedEOF) {
// 			return lastEntry.id, nil
// 		}
// 		if checkEOF(err) {
// 			return lastEntry.id, nil
// 		}
// 		if err != nil {
// 			return nil, err
// 		}

// 		lastEntry = entry
// 	}
// }

// func (reader *SSTableReader) peekNextId() ([]byte, error) {
// 	pos := 1
// 	var idSize int

// 	for {
// 		var result []byte
// 		result, err := reader.buffer.Peek(pos)
// 		if checkEOF(err) {
// 			return nil, err
// 		}
// 		if result[len(result)-1] == byte(0) {
// 			pos += 1
// 			continue
// 		}
// 		idSize = int(result[len(result)-1])
// 		break
// 	}

// 	content, err := reader.buffer.Peek(pos + idSize)
// 	id := content[pos:]

// 	if err != nil {
// 		return nil, err
// 	}

// 	return id, nil
// }

// func (reader *SSTableReader) readNextEntry() (Entry, error) {
// 	for { //If the size is 0, it's padding in a block. Keep looking until a new block or EOF
// 		idSize, err := reader.buffer.Peek(1)

// 		if err != nil {
// 			return Entry{}, err
// 		}

// 		if idSize[0] == byte(0) {
// 			reader.buffer.ReadByte() //Consume the zero byte
// 			continue
// 		}

// 		reader.buffer.Peek(config.BlockSize)
// 		break
// 	}

// 	entry := Entry{}
// 	entry.deserialize(reader.buffer)

// 	return entry, nil
// }

// func (reader *SSTableReader) reset() {
// 	if reader.rawBuffer != nil {
// 		reader.buffer = bufio.NewReader(bytes.NewReader(reader.rawBuffer.Bytes()))
// 	} else if reader.file != nil {
// 		reader.file.Seek(0, io.SeekStart)
// 		reader.buffer = bufio.NewReader(reader.file)
// 	}
// }

// // Method for testing, fully scans the table and returns the number of entires
// func (reader *SSTableReader) count() int {
// 	count := 0
// 	reader.reset()
// 	for {
// 		_, err := reader.readNextEntry()
// 		if checkEOF(err) {
// 			return count
// 		}
// 		count++
// 	}
// }

// func (reader *SSTableReader) scan(searchId []byte) (*Entry, error) {
// 	reader.reset()
// 	for {
// 		entry, err := reader.readNextEntry()

// 		if checkEOF(err) {
// 			return nil, nil
// 		}
// 		if err != nil {
// 			return nil, err
// 		}
// 		if !bytes.Equal(entry.id, searchId) {
// 			continue
// 		}
// 		return &entry, nil
// 	}
// }
