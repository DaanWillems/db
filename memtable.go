package main

import (
	"bytes"
	"container/list"
	"fmt"
)

type MemtableEntry struct {
	id      []byte
	values  [][]byte
	deleted bool
}

func (entry *MemtableEntry) Deserialize(bytes []byte) {
	index := 0
	id_size := int(bytes[index])
	index++
	entry.id = bytes[index:index+id_size]
	index += id_size

  deleted_i := int(bytes[index])
	entry.deleted = false
	if deleted_i == 1 {
   entry.deleted = true
	}

	index++
  values_count := int(bytes[index])
	index++
	for range values_count {
	  size := int(bytes[index])
		index++
		value := []byte{}
		for range size {
      value = append(value, bytes[index])
			index++
		}
    
		entry.values = append(entry.values, value)
	}
}

func (entry *MemtableEntry) Serialize() (int, []byte) {
	bytes := []byte{}
	bytes = append(bytes, byte(len(entry.id)))
	bytes = append(bytes, entry.id...)
	if entry.deleted {
		bytes = append(bytes, byte(1))
	} else {
		bytes = append(bytes, byte(0))
	}
	bytes = append(bytes, byte(len(entry.values)))
	for _, v := range entry.values {
		bytes = append(bytes, byte(int(len(v))))
		bytes = append(bytes, []byte(v)...)
	}
	return len(bytes), bytes
}

type Memtable struct {
	entries *list.List
}

func NewMemtable() Memtable {
	return Memtable{
		entries: list.New(),
	}
}

func (m *Memtable) Get(id []byte) *MemtableEntry {
	for e := m.entries.Front(); e != nil; e = e.Next() {
		next := e.Next()
		if next != nil && bytes.Equal(id, next.Value.(MemtableEntry).id) {
			entry := next.Value.(MemtableEntry)
			return &entry
		}
	}
	return nil
}

func (m *Memtable) Update(id []byte, values [][]byte) {

	entry := MemtableEntry{
		id:      id,
		values:  values,
		deleted: false,
	}

	for e := m.entries.Front(); e != nil; e = e.Next() {
		if bytes.Equal(e.Value.(MemtableEntry).id, id) {
			e.Value = entry
			return
		}
	}
	return
}

func (m *Memtable) Insert(id []byte, values [][]byte) {

	entry := MemtableEntry{
		id:      id,
		values:  values,
		deleted: false,
	}

	for e := m.entries.Front(); e != nil; e = e.Next() {
		next := e.Next()
		if next != nil && bytes.Compare(id, next.Value.(MemtableEntry).id) == -1 {
			m.entries.InsertBefore(entry, next)
			return
		}
	}

	m.entries.PushBack(entry)
}

func (m *Memtable) Flush() {

}

func (m *Memtable) Print() {
	for e := m.entries.Front(); e != nil; e = e.Next() {
		fmt.Printf("%v\n", e.Value)
	}
}
