package storage

import (
	"bytes"
	"os"
)

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
