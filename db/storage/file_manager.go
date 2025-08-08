package storage

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

type FileManager struct {
	loaded         bool
	ledgerFile     *os.File
	ledger         []string
	openReadFiles  map[string]*os.File
	openWriteFiles map[string]*os.File
}

var fileManager FileManager

func (fileManager *FileManager) close() {
	if fileManager.loaded {
		fileManager.ledgerFile.Close()
	}

	for _, fd := range fileManager.openReadFiles {
		err := fd.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, fd := range fileManager.openWriteFiles {
		err := fd.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

}

func initFileManager(path string) {
	fileManager = FileManager{
		loaded:         true,
		openReadFiles:  map[string]*os.File{},
		openWriteFiles: map[string]*os.File{},
	}

	indexFile, err := fileManager.openWriteFile(path)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(indexFile)

	dataFiles := []string{}
	for scanner.Scan() {
		dataFiles = append(dataFiles, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	fileManager.ledgerFile = indexFile
	fileManager.ledger = dataFiles
}

func (fileManager *FileManager) getDataIndex() []string {
	return fileManager.ledger
}

func (fileManager *FileManager) openWriteFile(path string) (*os.File, error) {
	if val, ok := fileManager.openWriteFiles[path]; ok {
		return val, nil
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	fileManager.openWriteFiles[path] = fd

	return fd, nil
}

func (fileManager *FileManager) openReadFile(path string) (*os.File, error) {
	if val, ok := fileManager.openReadFiles[path]; ok {
		return val, nil
	}
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fileManager.openReadFiles[path] = fd

	return fd, nil
}

func (fileManager *FileManager) addFileToLedger(fileName string) {
	fileManager.ledger = append(fileManager.ledger, fileName)
	fileManager.ledgerFile.Write([]byte(fileName + "\n"))
	fileManager.ledgerFile.Sync()
}

func (fileManager *FileManager) writeDataFile(memtable *Memtable) {
	if !fileManager.loaded {
		panic("databaseFileStructure is not loaded")
	}

	fileName := fmt.Sprintf("%v", time.Now().UnixNano())
	writer := newSSTableWriterFromPath(fmt.Sprintf("./%v/%v", config.DataDirectory, fileName))
	err := writer.writeFromMemtable(memtable)

	if err != nil {
		panic(err)
	}

	fileManager.addFileToLedger(fileName)
}
