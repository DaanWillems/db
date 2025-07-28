package database

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

type DatabaseFileStructure struct {
	loaded    bool
	indexFile *os.File
	index     []string
}

var databaseFileStructure DatabaseFileStructure

func CloseIndex() {
	if !databaseFileStructure.loaded {
		panic("databaseFileStructure is not loaded")
	}

	databaseFileStructure.indexFile.Close()
}

func LoadFileIndex() {
	indexFile, err := os.OpenFile("./data/index", os.O_RDWR|os.O_CREATE, 0644)
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

	databaseFileStructure = DatabaseFileStructure{
		indexFile: indexFile,
		index:     dataFiles,
		loaded:    true,
	}
}

func getDataIndex() []string {
	return databaseFileStructure.index
}

func addFileToIndex(fileName string) {
	databaseFileStructure.index = append(databaseFileStructure.index, fileName)
	databaseFileStructure.indexFile.Write([]byte(fileName + "\n"))
	databaseFileStructure.indexFile.Sync()
}

func WriteDataFile(table *SSTable) {
	if !databaseFileStructure.loaded {
		panic("databaseFileStructure is not loaded")
	}

	fileName := fmt.Sprintf("%v", time.Now().UnixNano())

	err := os.WriteFile(fmt.Sprintf("data/%v", fileName), *table.Blocks, 0644)
	addFileToIndex(fileName)

	if err != nil {
		panic(err)
	}
}
