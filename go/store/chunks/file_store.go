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
	defaultBuffSz  = 1024 * 16
)

func NewFileStore(path, ver string) (*FileStore, error) {
	path = filepath.Join(path, fileStoreName)

	flag := os.O_CREATE | os.O_RDWR
	perm := os.ModePerm | os.ModeExclusive

	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	wr := bufio.NewWriterSize(f, defaultBuffSz)
	idx := make(map[hash.Hash]indexEntry, defaultIndexSz)

	fs := &FileStore{
		idx: idx,
		rd:  f,
		wr:  wr,
		mu:  &sync.RWMutex{},
		ver: ver,
	}

	if err = populateFileStoreIndex(fs); err != nil {
		return nil, err
	}

	return fs, nil
}

type FileStore struct {
	root hash.Hash
	idx  map[hash.Hash]indexEntry
	mu   *sync.RWMutex

	rd  io.ReaderAt
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

func (fs *FileStore) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	e, ok := fs.idx[h]
	if !ok {
		return EmptyChunk, nil
	}

	return fs.readChunk(e)
}

func (fs *FileStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for h := range hashes {
		e, ok := fs.idx[h]
		if !ok {
			continue
		}

		c, err := fs.readChunk(e)
		if err != nil {
			return err
		}

		found(ctx, &c)
	}
	return nil
}

func (fs *FileStore) Has(ctx context.Context, h hash.Hash) (ok bool, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	_, ok = fs.idx[h]
	return
}

func (fs *FileStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	absent = hash.NewHashSet()
	for h := range hashes {
		if _, ok := fs.idx[h]; !ok {
			absent.Insert(h)
		}
	}
	return
}

func (fs *FileStore) Put(ctx context.Context, c Chunk) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.writeChunk(c)
}

func (fs *FileStore) Version() string {
	return fs.ver
}

func (fs *FileStore) Rebase(ctx context.Context) error {
	return nil
}

func (fs *FileStore) Root(ctx context.Context) (hash.Hash, error) {
	return fs.root, nil
}

func (fs *FileStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.root != last {
		return false, fmt.Errorf("hashes not equal %v != %v", fs.root, last)
	}

	if err := writeRootUpdateRecord(fs.wr, current); err != nil {
		return false, err
	}
	if err := fs.wr.Flush(); err != nil {
		return false, err
	}
	fs.root = current

	return true, nil
}

func (fs *FileStore) Stats() interface{} {
	return nil
}

func (fs *FileStore) StatsSummary() string {
	return ""
}

func (fs *FileStore) Close() error {
	return nil
}

func (fs *FileStore) readChunk(e indexEntry) (Chunk, error) {
	// we must flush before we can read
	if err := fs.wr.Flush(); err != nil {
		return Chunk{}, err
	}

	r, err := readChunkRecord(fs.rd, e)
	if err != nil {
		return Chunk{}, err
	}

	return NewChunkWithHash(r.key, r.data), nil
}

func (fs *FileStore) writeChunk(c Chunk) error {
	rec := chunkRecord{
		key:    c.Hash(),
		root:   fs.root,
		length: uint32(c.Size()),
		data:   c.Data(),
	}
	off := uint32(fs.pos)

	n, err := writeChunkRecord(fs.wr, rec)
	if err != nil {
		return err
	}
	fs.pos += int64(n)

	fs.idx[rec.key] = indexEntry{
		key:    rec.key,
		length: rec.length,
		offset: off,
	}
	return nil
}

const chunkRecordHeaderSz = 44

type chunkRecord struct {
	key    hash.Hash
	root   hash.Hash
	length uint32
	data   []byte
}

func (r chunkRecord) rootUpdateRecord() bool {
	return r.key == r.root && r.length == 0
}

func populateFileStoreIndex(fs *FileStore) error {
	var head [chunkRecordHeaderSz]byte
	for {
		n, err := fs.rd.ReadAt(head[:], fs.pos)
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
		offset := uint32(fs.pos)
		fs.pos += int64(rec.length + chunkRecordHeaderSz)

		if rec.rootUpdateRecord() {
			fs.root = rec.root
			continue
		}

		fs.idx[rec.key] = indexEntry{
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

func writeRootUpdateRecord(wr io.Writer, root hash.Hash) (err error) {
	_, err = writeChunkRecord(wr, chunkRecord{
		key:  root,
		root: root,
	})
	return err
}

func readChunkRecord(rd io.ReaderAt, e indexEntry) (r chunkRecord, err error) {
	buf := make([]byte, e.length+chunkRecordHeaderSz)

	var n int
	n, err = rd.ReadAt(buf, int64(e.offset))
	if err != nil {
		return r, err
	}
	if n < len(buf) {
		return r, fmt.Errorf("too few bytes read (%d vs %d)", n, len(buf))
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
	// todo(andy)
	return nil
}
