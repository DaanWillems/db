package database

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
)

type MemtableEntry struct {
	id      []byte
	values  [][]byte
	deleted bool
}

func (entry *MemtableEntry) Deserialize(entryBytes []byte) error {
	buf := bytes.NewBuffer(entryBytes)

	idLen, err := mustReadByte(buf)
	if err != nil {
		return err
	}

	id, err := mustReadN(buf, int(idLen))
	if err != nil {
		return err
	}
	entry.id = id

	_, err = mustReadByte(buf) //Discard content length
	if err != nil {
		return err
	}
	deleted_i, err := mustReadByte(buf)
	if err != nil {
		return err
	}
	entry.deleted = false
	if int(deleted_i) == 1 {
		entry.deleted = true
	}

	valuesCount, err := mustReadByte(buf)
	if err != nil {
		return err
	}
	for range valuesCount {
		valueLen, err := mustReadByte(buf)
		if err != nil {
			return err
		}
		value, err := mustReadN(buf, int(valueLen))
		if err != nil {
			return err
		}
		entry.values = append(entry.values, value)
	}
	return nil
}

func (entry *MemtableEntry) Serialize() (int, []byte) {
	var header bytes.Buffer
	var content bytes.Buffer

	header.WriteByte(byte(len(entry.id)))
	header.Write(entry.id)

	if entry.deleted {
		content.WriteByte(1)
	} else {
		content.WriteByte(0)
	}

	content.WriteByte(byte(len(entry.values)))
	for _, v := range entry.values {
		content.WriteByte(byte(len(v)))
		content.Write(v)
	}

	contentBytes := content.Bytes()
	header.WriteByte(byte(len(contentBytes)))

	bytes := append(header.Bytes(), content.Bytes()...)

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
		if bytes.Equal(id, e.Value.(MemtableEntry).id) {
			entry := e.Value.(MemtableEntry)
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

func (m *Memtable) Insert(entry MemtableEntry) {

	for e := m.entries.Front(); e != nil; e = e.Next() {
		next := e.Next()
		if next != nil && bytes.Compare(entry.id, next.Value.(MemtableEntry).id) == -1 {
			m.entries.InsertBefore(entry, next)
			return
		}
	}

	m.entries.PushBack(entry)
}

func (m Memtable) InsertRaw(id []byte, values [][]byte) {
	entry := MemtableEntry{
		id:      id,
		values:  values,
		deleted: false,
	}

	m.Insert(entry)
}

func mustReadN(buf *bytes.Buffer, n int) ([]byte, error) {
	b := make([]byte, n)
	readN, err := buf.Read(b)

	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if readN != n {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

func mustReadByte(buf *bytes.Buffer) (byte, error) {
	b, err := buf.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return b, nil
}

func (m *Memtable) Print() {
	for e := m.entries.Front(); e != nil; e = e.Next() {
		fmt.Printf("%v\n", e.Value)
	}
}
