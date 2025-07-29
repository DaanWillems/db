package storage

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMemtableOrderStr(t *testing.T) {
	memtable := NewMemtable()

	memtable.insertRaw([]byte("abc"), [][]byte{[]byte("a")})
	memtable.insertRaw([]byte("bdq"), [][]byte{[]byte("a")})
	memtable.insertRaw([]byte("abd"), [][]byte{[]byte("a")})

	entry := memtable.entries.Front()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte("abc")) {
		t.Errorf("Id does not match. Expected %v, got %s", "abc", entry.Value.(MemtableEntry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte("abd")) {
		t.Errorf("Id does not match. Expected %v, got %s", "abd", entry.Value.(MemtableEntry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte("bdq")) {
		t.Errorf("Id does not match. Expected %v, got %s", "bdq", entry.Value.(MemtableEntry).id)
	}
}

func TestMemtableOrderInt(t *testing.T) {
	memtable := NewMemtable()

	memtable.insertRaw([]byte{byte(1)}, [][]byte{[]byte("a")})
	memtable.insertRaw([]byte{byte(3)}, [][]byte{[]byte("a")})
	memtable.insertRaw([]byte{byte(2)}, [][]byte{[]byte("a")})

	entry := memtable.entries.Front()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte{byte(1)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 1, entry.Value.(MemtableEntry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte{byte(2)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 2, entry.Value.(MemtableEntry).id)
	}
	entry = entry.Next()
	if !bytes.Equal(entry.Value.(MemtableEntry).id, []byte{byte(3)}) {
		t.Errorf("Id does not match. Expected %d, got %d", 3, entry.Value.(MemtableEntry).id)
	}
}

func TestSerializeDeserializeStr(t *testing.T) {
	e := MemtableEntry{
		id:      []byte("abc"),
		values:  [][]byte{[]byte("a"), []byte("b")},
		deleted: false,
	}

	_, s := e.serialize()
	e1 := MemtableEntry{}
	e1.deserialize(s)

	if !reflect.DeepEqual(e, e1) {
		t.Errorf("Deserialized struct does not match original.\n Expected \n%v \n got \n%v", e, e1)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	e := MemtableEntry{
		id:      []byte{byte(9)},
		values:  [][]byte{[]byte("a"), []byte("b")},
		deleted: false,
	}

	_, s := e.serialize()
	e1 := MemtableEntry{}
	e1.deserialize(s)

	if !reflect.DeepEqual(e, e1) {
		t.Errorf("Deserialized struct does not match original.\n Expected \n%v \n got \n%v", e, e1)
	}
}
