package storage

import (
	"bufio"
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
)

type Memtable struct {
	entries *list.List
}

type Entry struct {
	id      []byte
	value   []byte
	deleted bool
}

func (entry *Entry) deserialize(buf *bufio.Reader) error {
	idLen, err := mustReadByte(buf)
	if err != nil {
		return err
	}

	id, err := mustReadN(buf, int(idLen))
	if err != nil {
		return err
	}
	entry.id = id

	deleted_i, err := mustReadByte(buf)
	if err != nil {
		return err
	}
	entry.deleted = false
	if int(deleted_i) == 1 {
		entry.deleted = true
	}

	valueLen, err := mustReadByte(buf)
	if err != nil {
		return err
	}

	value, err := mustReadN(buf, int(valueLen))
	if err != nil {
		return err
	}
	entry.value = value

	return nil
}

func (entry *Entry) serialize() (int, []byte) {
	var header bytes.Buffer
	var content bytes.Buffer

	header.WriteByte(byte(len(entry.id)))
	header.Write(entry.id)

	if entry.deleted {
		content.WriteByte(1)
	} else {
		content.WriteByte(0)
	}

	content.WriteByte(byte(len(entry.value)))
	content.Write(entry.value)

	bytes := append(header.Bytes(), content.Bytes()...)

	return len(bytes), bytes
}

func newMemtable() Memtable {
	return Memtable{
		entries: list.New(),
	}
}

func (m *Memtable) Get(id []byte) *Entry {
	for e := m.entries.Front(); e != nil; e = e.Next() {
		if bytes.Equal(id, e.Value.(Entry).id) {
			entry := e.Value.(Entry)
			return &entry
		}
	}
	return nil
}

func (m *Memtable) update(id []byte, value []byte) {

	entry := Entry{
		id:      id,
		value:   value,
		deleted: false,
	}

	for e := m.entries.Front(); e != nil; e = e.Next() {
		if bytes.Equal(e.Value.(Entry).id, id) {
			e.Value = entry
			return
		}
	}
}

func (m *Memtable) insert(entry Entry) {
	for e := m.entries.Front(); e != nil; e = e.Next() {
		next := e.Next()
		if next != nil && bytes.Compare(entry.id, next.Value.(Entry).id) == -1 {
			m.entries.InsertBefore(entry, next)
			return
		}
	}

	m.entries.PushBack(entry)
}

func (m Memtable) insertRaw(id []byte, value []byte) {
	entry := Entry{
		id:      id,
		value:   value,
		deleted: false,
	}

	m.insert(entry)
}

func mustReadN(buf *bufio.Reader, n int) ([]byte, error) {
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

func mustReadByte(buf *bufio.Reader) (byte, error) {
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
