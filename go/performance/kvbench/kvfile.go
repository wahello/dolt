// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvbench

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	fileStoreName = "kv.store"

	indexEntrySz   = 24
	defaultIndexSz = 1024
	defaultBuffSz  = 1024 * 16
)

type FileStore struct {
	rd  io.ReaderAt
	wr  *bufio.Writer
	pos int64

	idx  map[hash.Hash]indexEntry
	swap [24]byte

	mu *sync.RWMutex
}

var _ keyValStore = &FileStore{}

func NewFileStore(path string) keyValStore {
	path = filepath.Join(path, fileStoreName)

	flag := os.O_CREATE | os.O_RDWR
	flag |= os.O_TRUNC
	perm := os.ModePerm | os.ModeExclusive

	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		panic(err)
	}

	wr := bufio.NewWriterSize(f, defaultBuffSz)
	idx := make(map[hash.Hash]indexEntry, defaultIndexSz)

	kv := &FileStore{
		idx: idx,
		rd:  f,
		wr:  wr,
		mu:  &sync.RWMutex{},
	}

	if err = readIndex(kv); err != nil {
		panic(err)
	}

	return kv
}

func (fs *FileStore) get(key []byte) (val []byte, ok bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var e indexEntry
	e, ok = fs.idx[hashKey(key)]

	if ok {
		buf := make([]byte, e.length+indexEntrySz)

		n, err := fs.rd.ReadAt(buf, int64(e.offset))
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			panic(err)
		}

		header := readIndexEntry(buf[:indexEntrySz])
		val = buf[indexEntrySz:]

		if !equalIndexEntries(e, header) {
			panic("expected equal index entries")
		}
		if n < len(buf) {
			panic("bad read")
		}
	}
	return
}

func (fs *FileStore) put(key, value []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.writePair(key, value); err != nil {
		panic(err)
	}
	if err := fs.wr.Flush(); err != nil {
		panic(err)
	}
}

func (fs *FileStore) putMany(keys, values [][]byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for i := range keys {
		if err := fs.writePair(keys[i], values[i]); err != nil {
			panic(err)
		}
	}
	if err := fs.wr.Flush(); err != nil {
		panic(err)
	}
}

func (fs *FileStore) writePair(key, value []byte) (err error) {
	e := indexEntry{
		key:    hashKey(key),
		offset: int32(fs.pos),
		length: int32(len(value)),
	}
	writeIndexEntry(fs.swap[:], e)

	var n int
	n, err = fs.wr.Write(fs.swap[:])
	if err != nil {
		return err
	}
	fs.pos += int64(n)

	n, err = fs.wr.Write(value)
	if err != nil {
		return err
	}
	fs.pos += int64(n)

	fs.idx[e.key] = e
	return
}

func (fs *FileStore) delete(key []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	delete(fs.idx, hashKey(key))
}

func hashKey(key []byte) (h hash.Hash) {
	if len(key) > len(h) {
		key = key[:len(h)]
	}
	copy(h[:], key)
	return
}

func readIndex(store *FileStore) (err error) {
	var n int
	var e indexEntry
	for {
		n, err = store.rd.ReadAt(store.swap[:], store.pos)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return
		}
		store.pos += int64(n)

		e = readIndexEntry(store.swap[:])
		e.offset = int32(store.pos)
		store.idx[e.key] = e

		// skip over value data
		store.pos += int64(e.length)
	}
}

type indexEntry struct {
	key    hash.Hash
	offset int32
	length int32
}

func equalIndexEntries(l, r indexEntry) bool {
	return l.key == r.key &&
		l.length == r.length
}

func readIndexEntry(buf []byte) (e indexEntry) {
	assertSize(buf, indexEntrySz)
	e.key = hash.New(buf[:20])
	l := binary.LittleEndian.Uint32(buf[20:])
	e.length = int32(l)
	return
}

func writeIndexEntry(buf []byte, e indexEntry) {
	assertSize(buf, indexEntrySz)
	copy(buf[:20], e.key[:])
	binary.LittleEndian.PutUint32(buf[20:], uint32(e.length))
}

func assertSize(buf []byte, sz int) {
	if len(buf) != sz {
		panic(fmt.Sprintf("expected size %d", sz))
	}
}
