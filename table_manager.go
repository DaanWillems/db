package main

import "fmt"

type Table struct {
	memtable Memtable
}


type TableManager struct {
	tables map[string]Table
}

func NewTableManager() TableManager {
  //Load table structure from disk
  tm := TableManager{}
	tm.initSchemaTable()
	return tm
}

func (tm *TableManager) CreateTable(name string, columns []DbType) {
  tm.tables[name] = Table{
		NewMemtable(),
	}

	tm.initSchemaTable()
}

func (tm *TableManager) initSchemaTable() {
	tm.tables = map[string]Table{}
	schemaTable := Table{
		NewMemtable(),
	}

  name := DbString{"Schema"}
	columnTypes := DbString{"string"}

	schemaTable.memtable.Insert(name.Bytes(), [][]byte{columnTypes.Bytes()})

	tm.tables["Schema"] = schemaTable
}

func (tm *TableManager) addSchema(name string, columns []DbType) {
	schemaTable := tm.tables["Schema"]
	id := DbString{name}
	columnTypes := ""

  for _, columnType := range columns {
		columnTypes += columnType.String() + ","
	}

	schemaTable.memtable.Insert(id.Bytes(), [][]byte{[]byte(columnTypes)})

	tm.tables[name] = Table{
		NewMemtable(),
	}
}

func (tm *TableManager) Insert(tableName string, id Serializable, values []Serializable) {
	valueBytes := [][]byte{}
	for _, val := range values {
    valueBytes = append(valueBytes, val.Bytes())
	}

	table := tm.tables[tableName]
	table.memtable.Insert(id.Bytes(), valueBytes)
}

func (tm *TableManager) Query(tableName string, idFilter Serializable) {
	table := tm.tables[tableName]
	entry := table.memtable.Get(idFilter.Bytes())
	fmt.Printf("%v", entry)
	fmt.Println("")
	//How to interpret entry?

}
