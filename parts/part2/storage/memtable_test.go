package storage

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMemtableOrderStr(t *testing.T) {
	memtable := newMemtable()

	memtable.insertRaw([]byte("abc"), []byte("a"))
	memtable.insertRaw([]byte("bdq"), []byte("a"))
	memtable.insertRaw([]byte("abd"), []byte("a"))

	entry := memtable.entries.Front()
	if !bytes.Equal(entry.Value.(Entry).id, []byte("abc")) {
		t.Errorf("Id does not match. Expected %v, got %s", "abc", entry.Value.(Entry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(Entry).id, []byte("abd")) {
		t.Errorf("Id does not match. Expected %v, got %s", "abd", entry.Value.(Entry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(Entry).id, []byte("bdq")) {
		t.Errorf("Id does not match. Expected %v, got %s", "bdq", entry.Value.(Entry).id)
	}
}

func TestMemtableOrderInt(t *testing.T) {
	memtable := newMemtable()

	memtable.insertRaw([]byte{byte(1)}, []byte("a"))
	memtable.insertRaw([]byte{byte(3)}, []byte("a"))
	memtable.insertRaw([]byte{byte(2)}, []byte("a"))

	entry := memtable.entries.Front()
	if !bytes.Equal(entry.Value.(Entry).id, []byte{byte(1)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 1, entry.Value.(Entry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(Entry).id, []byte{byte(2)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 2, entry.Value.(Entry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(Entry).id, []byte{byte(3)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 3, entry.Value.(Entry).id)
	}
}

func TestSerializeDeserializeStr(t *testing.T) {
	e := Entry{
		id:      []byte("abc"),
		value:   []byte("abcd"),
		deleted: false,
	}

	_, s := e.serialize()
	e1 := Entry{}
	e1.deserialize(s)

	if !reflect.DeepEqual(e, e1) {
		t.Errorf("Deserialized struct does not match original.\n Expected \n%v \n got \n%v", e, e1)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	e := Entry{
		id:      IntToBytes(368),
		value:   IntToBytes(368),
		deleted: false,
	}

	_, s := e.serialize()
	e1 := Entry{}
	e1.deserialize(s)

	if !reflect.DeepEqual(e, e1) {
		t.Errorf("Deserialized struct does not match original.\n Expected \n%v \n got \n%v", e, e1)
	}
}
