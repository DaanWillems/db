package main

type DbType int

const (
	DbStringType DbType = iota
	DbBoolType
	DbInt8Type
)

var dbTypeName = map[DbType]string{
	DbStringType: "string",
	DbBoolType:   "bool",
	DbInt8Type:   "int8",
}


func (t DbType) String() string {
  return dbTypeName[t]
}

type Serializable interface {
	Bytes() []byte
}

type DbString struct {
  Value string
}

func (s DbString) Bytes() []byte {
	return []byte(s.Value)
}

type DbInt8 struct {
  Value int
}

func (i DbInt8) Bytes() []byte {
	return []byte{byte(i.Value)}
}

type DbBool struct {
	Value bool
}

func (b DbBool) Bytes() []byte {
	if b.Value {
		return []byte{byte(1)}
	}
	return []byte{byte(0)}
}
