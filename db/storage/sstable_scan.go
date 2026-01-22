package storage

import (
	"bytes"
	"log"
	"os"
)

type SSTableScanner struct {
	path string
	file *os.File
}

func newSSTableScannerFromPath(path string) *SSTableScanner {
	fd, err := fileManager.openReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	return &SSTableScanner{
		path: path,
		file: fd,
	}
}

func (scanner *SSTableScanner) getLastID() ([]byte, error) {
	it := NewSSTableIteratorFromPath(scanner.path)
	for it.Next() {
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return it.Entry().id, nil
}

func (scanner *SSTableScanner) getFirstID() ([]byte, error) {
	it := NewSSTableIteratorFromPath(scanner.path)
	it.Next()

	if err := it.Error(); err != nil {
		return nil, err
	}

	return it.Entry().id, nil
}

func (scanner *SSTableScanner) scan(id []byte) (*Entry, error) {
	it := NewSSTableIteratorFromPath(scanner.path)

	for it.Next() {
		if bytes.Equal(it.Entry().id, id) {
			return it.Entry(), nil
		}
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return nil, nil
}
