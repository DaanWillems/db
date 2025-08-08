package storage

import (
	"bufio"
	"fmt"
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

func (fileLedger *FileManager) close() {
	if !fileLedger.loaded {
		panic("File ledger is not loaded")
	}

	fileLedger.ledgerFile.Close()
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

func (fileLedger *FileManager) getDataIndex() []string {
	return fileLedger.ledger
}

func (fileLedger *FileManager) openWriteFile(path string) (*os.File, error) {
	if val, ok := fileLedger.openWriteFiles[path]; ok {
		return val, nil
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	fileLedger.openWriteFiles[path] = fd

	return fd, nil
}

func (fileLedger *FileManager) openReadFile(path string) (*os.File, error) {
	if val, ok := fileLedger.openReadFiles[path]; ok {
		return val, nil
	}
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fileLedger.openReadFiles[path] = fd

	return fd, nil
}

func (fileLedger *FileManager) addFileToLedger(fileName string) {
	fileLedger.ledger = append(fileLedger.ledger, fileName)
	fileLedger.ledgerFile.Write([]byte(fileName + "\n"))
	fileLedger.ledgerFile.Sync()
}

func (fileLedger *FileManager) writeDataFile(memtable *Memtable) {
	if !fileLedger.loaded {
		panic("databaseFileStructure is not loaded")
	}

	fileName := fmt.Sprintf("%v", time.Now().UnixNano())
	fd, _ := os.Create(fmt.Sprintf("./%v/%v", config.DataDirectory, fileName))
	writer := newSSTableWriter(bufio.NewWriter(fd))
	err := writer.writeFromMemtable(memtable)

	if err != nil {
		panic(err)
	}

	fileLedger.addFileToLedger(fileName)
}
