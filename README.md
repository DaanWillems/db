# Simple Go Key-Value Database

This project is a simple key-value database implemented in Go. It features an in-memory table (memtable), write-ahead logging (WAL), and persistent storage using SSTables. The database supports basic schema management and serialization of different data types.

## Features

- **Memtable**: In-memory storage for fast reads and writes.
- **SSTable**: Persistent, sorted storage for efficient lookups.
- **Write-Ahead Log (WAL)**: Ensures durability and crash recovery.
- **Schema Management**: Define tables and columns with types.
- **Serialization**: Custom serialization for string, int8, and bool types.

## Project Structure

```
.gitignore
go.mod
main.go
memtable.go
memtable_test.go
sstable.go
sstable_test.go
table_manager.go
util.go
wal.go
parser/
  ast.go
  lexer.go
  parser.go
```

## Usage

### Build

```sh
go build
```

### Run

```sh
go run .
```

### Test

```sh
go test
```

## Example

See `main.go` for a usage example:

```go
tableManager := NewTableManager()
tableManager.addSchema("Test", []DbType{DbStringType, DbBoolType})

str := DbString{"a"}
val := []Serializable{DbBool{Value: true}}

tableManager.Insert("Test", str, val)
tableManager.Query("Test", str)
```

## License

MIT