package main

func main() {
	tableManager := NewTableManager()
	tableManager.addSchema("Test", []DbType{DbStringType, DbBoolType})

	str := DbString{"a"}
	val := []Serializable{DbBool{Value: true}}

	tableManager.Insert("Test", str, val)
	tableManager.Query("Test", str)
}
