package storage

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

type FileLedger struct {
	loaded     bool
	ledgerFile *os.File
	ledger     []string
}

var fileLedger FileLedger

func closeLedger() {
	if !fileLedger.loaded {
		panic("File ledger is not loaded")
	}

	fileLedger.ledgerFile.Close()
}

func loadFileLedger() {
	indexFile, err := os.OpenFile("./data/ledger", os.O_RDWR|os.O_CREATE, 0644)
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

	fileLedger = FileLedger{
		ledgerFile: indexFile,
		ledger:     dataFiles,
		loaded:     true,
	}
}

func getDataIndex() []string {
	return fileLedger.ledger
}

func addFileToLedger(fileName string) {
	fileLedger.ledger = append(fileLedger.ledger, fileName)
	fileLedger.ledgerFile.Write([]byte(fileName + "\n"))
	fileLedger.ledgerFile.Sync()
}

func writeDataFile(memtable *Memtable) {
	if !fileLedger.loaded {
		panic("databaseFileStructure is not loaded")
	}

	fileName := fmt.Sprintf("%v", time.Now().UnixNano())
	writer := newSSTableWriter(fmt.Sprintf("./data/%v", fileName))
	err := writer.writeFromMemtable(memtable)

	if err != nil {
		panic(err)
	}

	addFileToLedger(fileName)
}
