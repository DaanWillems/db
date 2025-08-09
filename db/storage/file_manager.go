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
	ledger         map[int][]string
	openReadFiles  map[string]*os.File
	openWriteFiles map[string]*os.File
}

var fileManager FileManager

func (fileManager *FileManager) close() {

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

func initFileManager(rootPath string) error {
	fileManager = FileManager{
		loaded:         true,
		openReadFiles:  map[string]*os.File{},
		openWriteFiles: map[string]*os.File{},
		ledger:         map[int][]string{},
	}
	err := os.RemoveAll(rootPath) //Temporary for testing
	err = os.Mkdir(rootPath, 0644)
	err = os.Mkdir(rootPath+"/tmp", 0644)
	if err != nil {
		return err
	}

	for i := range config.CompactionLevels {
		subPath := fmt.Sprintf("%v/%v", rootPath, i)
		err := os.Mkdir(subPath, 0644)
		if err != nil {
			return err
		}

		ledgerFile, err := fileManager.openWriteFile(fmt.Sprintf("%v/%v", subPath, "ledger"))
		if err != nil {
			panic(err)
		}

		scanner := bufio.NewScanner(ledgerFile)

		dataFiles := []string{}
		for scanner.Scan() {
			dataFiles = append(dataFiles, scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			return err
		}

		fileManager.ledger[i] = dataFiles
	}

	return nil
}

func (fileManager *FileManager) getNextFilename() string {
	return fmt.Sprintf("%d.sst", time.Now().UnixNano())
}

func (fileManager *FileManager) getDataIndex() map[int][]string {
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

func (fileManager *FileManager) addFileToLedger(fileName string, level int) error { //TODO: Make sure everything except L0 is sorted
	//TODO: Make this operation atomic by using tmp files
	file, err := fileManager.openWriteFile(fmt.Sprintf("%v/%v/%v", config.DataDirectory, level, "ledger"))
	if err != nil {
		return err
	}
	fullPath := fmt.Sprintf("%v/%v/%v", config.DataDirectory, level, fileName)
	fileManager.ledger[level] = append(fileManager.ledger[level], fullPath)
	file.Write([]byte(fullPath + "\n"))
	file.Sync()

	return nil
}

func (fileManager *FileManager) storeMemtable(memtable *Memtable) {
	if !fileManager.loaded {
		panic("databaseFileStructure is not loaded")
	}

	fileName := fmt.Sprintf("%v.sst", len(fileManager.ledger[0])) //TODO: Generate new file name appropriately
	writer := newSSTableWriterFromPath(fmt.Sprintf("./%v/0/%v", config.DataDirectory, fileName))
	err := writer.writeFromMemtable(memtable)

	if err != nil {
		panic(err)
	}

	fileManager.addFileToLedger(fileName, 0)
}
