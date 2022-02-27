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

package chunks

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	fileStoreName = "chunk.store"

	defaultIndexSz = 1024
	defaultBuffSz  = 1024 * 1024
)

var store *FileStore

func NewFileStore(path, ver string) (*FileStore, error) {
	// TODO(andy): this is gross and bad
	if store != nil {
		return store, nil
	}

	path = filepath.Join(path, fileStoreName)

	flag := os.O_CREATE | os.O_RDWR | os.O_APPEND
	perm := os.ModePerm | os.ModeExclusive

	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	wr := bufio.NewWriterSize(f, defaultBuffSz)
	idx := make(map[hash.Hash]indexEntry, defaultIndexSz)

	store = &FileStore{
		idx: idx,
		f:   f,
		wr:  wr,
		mu:  &sync.RWMutex{},
		ver: ver,
	}

	if err = populateFileStoreIndex(store); err != nil {
		return nil, err
	}

	return store, nil
}

type FileStore struct {
	root hash.Hash
	idx  map[hash.Hash]indexEntry
	mu   *sync.RWMutex

	f   *os.File
	wr  *bufio.Writer
	pos int64

	ver string
}

var _ ChunkStore = &FileStore{}

type indexEntry struct {
	key    hash.Hash
	length uint32
	offset uint32
}

func (st *FileStore) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	e, ok := st.idx[h]
	if !ok {
		return EmptyChunk, nil
	}

	return st.readChunk(e)
}

func (st *FileStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error {
	st.mu.RLock()
	defer st.mu.RUnlock()

	for h := range hashes {
		e, ok := st.idx[h]
		if !ok {
			continue
		}

		c, err := st.readChunk(e)
		if err != nil {
			return err
		}

		found(ctx, &c)
	}
	return nil
}

func (st *FileStore) Has(ctx context.Context, h hash.Hash) (ok bool, err error) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	_, ok = st.idx[h]
	return
}

func (st *FileStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	absent = hash.NewHashSet()
	for h := range hashes {
		if _, ok := st.idx[h]; !ok {
			absent.Insert(h)
		}
	}
	return
}

func (st *FileStore) Put(ctx context.Context, c Chunk) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.writeChunk(c)
}

func (st *FileStore) Version() string {
	return st.ver
}

func (st *FileStore) Rebase(ctx context.Context) error {
	return nil
}

func (st *FileStore) Root(ctx context.Context) (hash.Hash, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.root, nil
}

func (st *FileStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.root != last {
		return false, fmt.Errorf("hashes not equal %v != %v", st.root, last)
	}

	if err := st.writeRootUpdate(current); err != nil {
		return false, err
	}
	if err := st.wr.Flush(); err != nil {
		return false, err
	}
	st.root = current

	return true, nil
}

func (st *FileStore) Stats() interface{} {
	return nil
}

func (st *FileStore) StatsSummary() string {
	return ""
}

func (st *FileStore) Close() error {
	return st.f.Close()
}

func (st *FileStore) readChunk(e indexEntry) (Chunk, error) {
	// we must flush before we can read
	if err := st.wr.Flush(); err != nil {
		return Chunk{}, err
	}

	r, err := readChunkRecord(st.f, e)
	if err != nil {
		return Chunk{}, err
	}

	return NewChunkWithHash(r.key, r.data), nil
}

func (st *FileStore) writeChunk(c Chunk) error {
	rec := chunkRecord{
		key:    c.Hash(),
		root:   st.root,
		length: uint32(c.Size()),
		data:   c.Data(),
	}
	off := uint32(st.pos)

	n, err := writeChunkRecord(st.wr, rec)
	if err != nil {
		return err
	}
	st.pos += int64(n)

	st.idx[rec.key] = indexEntry{
		key:    rec.key,
		length: rec.length,
		offset: off,
	}
	return nil
}

func (st *FileStore) writeRootUpdate(root hash.Hash) error {
	n, err := writeChunkRecord(st.wr, chunkRecord{
		key:  root,
		root: root,
	})
	if err != nil {
		return err
	}

	st.pos += int64(n)
	return nil
}

const chunkRecordHeaderSz = 44

type chunkRecord struct {
	key    hash.Hash
	root   hash.Hash
	length uint32
	data   []byte
}

func (r chunkRecord) isRootUpdateRecord() bool {
	return r.key == r.root && r.length == 0
}

func populateFileStoreIndex(store *FileStore) error {
	var head [chunkRecordHeaderSz]byte
	for {
		n, err := store.f.ReadAt(head[:], store.pos)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if n != chunkRecordHeaderSz {
			panic("incorrect read size")
		}

		rec := readChunkRecordHeader(head[:])

		// advance to the next record
		offset := uint32(store.pos)
		store.pos += int64(rec.length + chunkRecordHeaderSz)

		if rec.isRootUpdateRecord() {
			store.root = rec.root
			continue
		}

		store.idx[rec.key] = indexEntry{
			key:    rec.key,
			length: rec.length,
			offset: offset,
		}
	}
}

func writeChunkRecord(wr io.Writer, rec chunkRecord) (n int, err error) {
	var k int
	if k, err = wr.Write(rec.key[:]); err != nil {
		return 0, err
	}
	n += k

	if k, err = wr.Write(rec.root[:]); err != nil {
		return 0, err
	}
	n += k

	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], rec.length)
	if k, err = wr.Write(length[:]); err != nil {
		return 0, err
	}
	n += k

	if k, err = wr.Write(rec.data); err != nil {
		return 0, err
	}
	n += k

	return
}

func readChunkRecord(rd io.ReaderAt, e indexEntry) (r chunkRecord, err error) {
	buf := make([]byte, e.length+chunkRecordHeaderSz)

	var n int
	n, err = rd.ReadAt(buf, int64(e.offset))
	if err != nil {
		return r, err
	}
	if n != len(buf) {
		return r, io.ErrShortWrite
	}

	r = readChunkRecordHeader(buf)
	r.data = buf[chunkRecordHeaderSz:]

	if err = validateChunkRead(e, r); err != nil {
		return r, err
	}

	return r, nil
}

func readChunkRecordHeader(buf []byte) chunkRecord {
	return chunkRecord{
		key:    hash.New(buf[:20]),
		root:   hash.New(buf[20:40]),
		length: binary.LittleEndian.Uint32(buf[40:44]),
	}
}

func validateChunkRead(e indexEntry, r chunkRecord) error {
	ok := e.length == r.length &&
		int(e.length) == len(r.data) &&
		e.key == r.key
	if !ok {
		panic("expected ok")
	}
	return nil
}
