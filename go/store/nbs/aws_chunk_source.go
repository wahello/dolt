// Copyright 2019-2021 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type indexParserF func([]byte) (tableIndex, error)

func newAWSChunkSource(ctx context.Context, ddb *ddbTableStore, s3 *s3ObjectReader, al awsLimits, name addr, chunkCount uint32, indexCache *indexCache, stats *Stats, parseIndex indexParserF) (cs chunkSource, err error) {
	if indexCache != nil {
		indexCache.lockEntry(name)
		defer func() {
			unlockErr := indexCache.unlockEntry(name)

			if err == nil {
				err = unlockErr
			}
		}()

		if index, found := indexCache.get(name); found {
			tra := &awsTableReaderAt{al: al, ddb: ddb, s3: s3, name: name, chunkCount: chunkCount}
			tr, err := newTableReader(index, tra, s3BlockSize)
			if err != nil {
				return &chunkSourceAdapter{}, err
			}
			return &chunkSourceAdapter{tr, name}, nil
		}
	}

	t1 := time.Now()
	indexBytes, tra, err := func() ([]byte, tableReaderAt, error) {
		if al.tableMayBeInDynamo(chunkCount) {
			data, err := ddb.ReadTable(ctx, name, stats)

			if data == nil && err == nil { // There MUST be either data or an error
				return nil, &dynamoTableReaderAt{}, errors.New("no data available")
			}

			if data != nil {
				return data, &dynamoTableReaderAt{ddb: ddb, h: name}, nil
			}

			if _, ok := err.(tableNotInDynamoErr); !ok {
				return nil, &dynamoTableReaderAt{}, err
			}
		}

		size := indexSize(chunkCount) + footerSize
		buff := make([]byte, size)

		n, _, err := s3.ReadFromEnd(ctx, name, buff, stats)

		if err != nil {
			return nil, &dynamoTableReaderAt{}, err
		}

		if size != uint64(n) {
			return nil, &dynamoTableReaderAt{}, errors.New("failed to read all data")
		}

		return buff, &s3TableReaderAt{s3: s3, h: name}, nil
	}()

	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	stats.IndexBytesPerRead.Sample(uint64(len(indexBytes)))
	stats.IndexReadLatency.SampleTimeSince(t1)

	index, err := parseIndex(indexBytes)

	if err != nil {
		return emptyChunkSource{}, err
	}

	if ohi, ok := index.(onHeapTableIndex); indexCache != nil && ok {
		indexCache.put(name, ohi)
	}

	tr, err := newTableReader(index, tra, s3BlockSize)
	if err != nil {
		return &chunkSourceAdapter{}, err
	}
	return &chunkSourceAdapter{tr, name}, nil
}

type awsTableReaderAt struct {
	once     sync.Once
	getTRErr error
	tra      tableReaderAt

	al  awsLimits
	ddb *ddbTableStore
	s3  *s3ObjectReader

	name       addr
	chunkCount uint32
}

func (atra *awsTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (int, error) {
	atra.once.Do(func() {
		atra.tra, atra.getTRErr = atra.getTableReaderAt(ctx, stats)
	})

	if atra.getTRErr != nil {
		return 0, atra.getTRErr
	}

	return atra.tra.ReadAtWithStats(ctx, p, off, stats)
}

func (atra *awsTableReaderAt) getTableReaderAt(ctx context.Context, stats *Stats) (tableReaderAt, error) {
	if atra.al.tableMayBeInDynamo(atra.chunkCount) {
		data, err := atra.ddb.ReadTable(ctx, atra.name, stats)

		if data == nil && err == nil { // There MUST be either data or an error
			return &dynamoTableReaderAt{}, errors.New("no data available")
		}

		if data != nil {
			return &dynamoTableReaderAt{ddb: atra.ddb, h: atra.name}, nil
		}

		if _, ok := err.(tableNotInDynamoErr); !ok {
			return &dynamoTableReaderAt{}, err
		}
	}

	return &s3TableReaderAt{s3: atra.s3, h: atra.name}, nil
}

// Janky s3 chunk source with disk index
func newAWSChunkSourceWithDiskIndex(ctx context.Context, s3 *s3ObjectReader, name addr, chunkCount uint32, indexCache *indexCache, stats *Stats) (cs chunkSource, err error) {

	size := indexSize(chunkCount) + footerSize

	// Download index to file
	tableFileSize, srcFilename, err := func() (uint64, string, error) {
		// TODO: Use `name` in filename?
		indexFile, err := os.CreateTemp(".", fmt.Sprintf("%s-index-len-*", name.String()))
		if err != nil {
			return 0, "", err
		}
		defer indexFile.Close()
		err = indexFile.Truncate(int64(size))
		if err != nil {
			return 0, "", err
		}
		_, tableFileSize, err := s3.ReadFromEndToFile(ctx, name, int(size), indexFile, stats)
		if err != nil {
			return 0, "", err
		}
		err = indexFile.Sync()
		if err != nil {
			return 0, "", err
		}
		return tableFileSize, indexFile.Name(), err
	}()
	if err != nil {
		return &chunkSourceAdapter{}, nil
	}

	// Convert lengths to offsets
	filename, err := func() (string, error) {
		srcFile, err := os.Open(srcFilename)
		if err != nil {
			return "", err
		}
		defer srcFile.Close()

		dstFile, err := os.CreateTemp(".", fmt.Sprintf("%s-index-off-*", name.String()))
		if err != nil {
			return "", err
		}
		defer dstFile.Close()

		trans := NewIndexFileTransformer(srcFile, chunkCount)
		_, err = io.Copy(dstFile, trans)
		if err != nil {
			return "", err
		}

		err = dstFile.Sync()
		if err != nil {
			return "", err
		}

		return dstFile.Name(), nil
	}()
	if err != nil {
		return &chunkSourceAdapter{}, nil
	}

	err = os.Remove(srcFilename)
	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	tra := &s3TableReaderAt{s3: s3, h: name}

	diskTableIndex, err := NewDiskTableIndexForFile(filename, tableFileSize)
	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	tr, err := newTableReader(diskTableIndex, tra, s3BlockSize)
	if err != nil {
		return &chunkSourceAdapter{}, err
	}

	return &chunkSourceAdapter{tr, name}, nil
}

type readerCloserAt interface {
	io.ReaderAt
	io.Closer
}

type diskTableIndex struct {
	tableFileSize uint64

	r                     readerCloserAt
	chunkCount            uint32
	tuplesSize            int64
	offsetsSize           int64
	suffixesSize          int64
	indexSize             uint64
	totalUncompressedData uint64
}

var _ tableIndex = &diskTableIndex{}

// NewDiskTableIndexForFile inits a disk table index using the given file of a table index and footer
func NewDiskTableIndexForFile(filename string, tableFileSize uint64) (diskTableIndex, error) {
	file, err := os.Open(filename)
	if err != nil {
		return diskTableIndex{}, err
	}

	chunkCount, totalUncompressedData, err := ReadTableFooter(file)
	if err != nil {
		return diskTableIndex{}, err
	}

	return NewDiskTableIndex(file, tableFileSize, chunkCount, totalUncompressedData), nil
}

func NewDiskTableIndex(r readerCloserAt, tableFileSize uint64, chunkCount uint32, totalUncompressedData uint64) diskTableIndex {
	suffixesSize := int64(chunkCount) * addrSuffixSize
	offsetsSize := int64(chunkCount) * offsetSize
	tuplesSize := int64(chunkCount) * prefixTupleSize
	indexSize := uint64(suffixesSize + offsetsSize + tuplesSize)

	return diskTableIndex{
		tableFileSize:         tableFileSize,
		r:                     r,
		chunkCount:            chunkCount,
		tuplesSize:            tuplesSize,
		offsetsSize:           offsetsSize,
		suffixesSize:          suffixesSize,
		indexSize:             indexSize,
		totalUncompressedData: totalUncompressedData,
	}
}

func (ti diskTableIndex) ChunkCount() uint32 {
	return ti.chunkCount
}

func (ti diskTableIndex) EntrySuffixMatches(idx uint32, h *addr) (bool, error) {
	off := ti.tuplesSize + ti.offsetsSize + int64(addrSuffixSize*idx)
	b := make([]byte, addrSuffixSize)
	_, err := ti.r.ReadAt(b, off)
	if err != nil {
		return false, err
	}

	return bytes.Equal(h[addrPrefixSize:], b), nil
}

func (ti diskTableIndex) IndexEntry(idx uint32, a *addr) (entry indexEntry, err error) {
	prefix, ord, err := ti.tupleAt(int64(idx))
	if err != nil {
		return &indexResult{}, err
	}

	if a != nil {
		binary.BigEndian.PutUint64(a[:], prefix)

		o := ti.tuplesSize + ti.offsetsSize + int64(addrSuffixSize*ord)
		suff := make([]byte, addrSuffixSize)
		_, err = ti.r.ReadAt(suff, o)
		if err != nil {
			return indexResult{}, err
		}
		copy(a[addrPrefixSize:], suff)
	}

	return ti.indexEntryOrd(ord)
}

func (ti diskTableIndex) indexEntryOrd(ord uint32) (entry indexEntry, err error) {
	getOff := func(pos int64) (uint64, error) {
		oBytes := make([]byte, offsetSize)
		_, err := ti.r.ReadAt(oBytes, pos)
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(oBytes), nil
	}

	ordPos := ti.tuplesSize + int64(offsetSize*ord)
	prevPos := ordPos - offsetSize

	var prevOff uint64
	if ord == 0 {
		prevOff = 0
	} else {
		prevOff, err = getOff(prevPos)
		if err != nil {
			return indexResult{}, err
		}
	}

	ordOff, err := getOff(ordPos)
	if err != nil {
		return indexResult{}, err
	}
	length := uint32(ordOff - prevOff)

	return indexResult{
		o: prevOff,
		l: length,
	}, nil
}

func (ti diskTableIndex) Lookup(h *addr) (entry indexEntry, found bool, err error) {
	tPre := h.Prefix()
	// TODO: Use bloom filter to fail fast
	idx := int64(GuessPrefixIdx(h.Prefix(), ti.chunkCount))
	pre, err := ti.prefixAt(uint32(idx))
	if err != nil {
		return indexResult{}, false, err
	}
	var ord uint32
	if pre == tPre {
		ord, found, err = ti.scanD(-1, idx, true, h)
		if found || err != nil {
			goto DONE
		}
		ord, found, err = ti.scanD(1, idx+1, true, h)
	} else if pre < tPre {
		ord, found, err = ti.scanD(1, idx+1, false, h)
	} else {
		ord, found, err = ti.scanD(-1, idx-1, false, h)
	}
DONE:
	if err != nil {
		return &indexResult{}, false, err
	}
	if !found {
		return &indexResult{}, false, err
	}
	e, err := ti.indexEntryOrd(ord)
	if err != nil {
		return indexResult{}, false, err
	}
	return e, true, err
}

func (ti diskTableIndex) scanD(d int, idx int64, foundP bool, h *addr) (ord uint32, found bool, err error) {
	prefix := h.Prefix()
	for ; 0 <= idx && idx < int64(ti.chunkCount); idx += int64(d) {
		currP, currO, err := ti.tupleAt(idx)
		if err != nil {
			return 0, false, err
		}
		if foundP && currP != prefix {
			// Stop early
			return 0, false, nil
		}
		if currP != prefix {
			continue
		}
		foundP = true
		m, err := ti.EntrySuffixMatches(currO, h)
		if err != nil {
			return 0, false, err
		}
		if m {
			return currO, true, err
		}
	}

	return 0, false, nil
}

func (ti diskTableIndex) tupleAt(idx int64) (prefix uint64, ord uint32, err error) {
	off := prefixTupleSize * idx
	b := make([]byte, prefixTupleSize)
	_, err = ti.r.ReadAt(b, off)
	if err != nil {
		return 0, 0, err
	}

	prefix = binary.BigEndian.Uint64(b[:])
	ord = binary.BigEndian.Uint32(b[addrPrefixSize:])
	return prefix, ord, err
}

func (ti diskTableIndex) prefixAt(idx uint32) (uint64, error) {
	off := int64(prefixTupleSize * idx)

	b := make([]byte, addrPrefixSize)
	_, err := ti.r.ReadAt(b, off)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(b), nil
}

func (ti diskTableIndex) Ordinals() ([]uint32, error) {
	// TODO:
	return []uint32{}, nil
}

func (ti diskTableIndex) Prefixes() ([]uint64, error) {
	// TODO:
	return []uint64{}, nil
}

func (ti diskTableIndex) TableFileSize() uint64 {
	return ti.tableFileSize
}

func (ti diskTableIndex) TotalUncompressedData() uint64 {
	return ti.totalUncompressedData
}

func (ti diskTableIndex) Close() error {
	err := ti.r.Close()
	if err != nil {
		return err
	}

	return nil
}

func (ti diskTableIndex) Clone() (tableIndex, error) {
	file, ok := ti.r.(*os.File)
	if !ok {
		panic("expected reader to be a `*os.File`")
	}

	filename, err := copyFile(file)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return diskTableIndex{
		r: f,
	}, nil
}

// Copies an open file and returns the copies filename
func copyFile(in *os.File) (string, error) {
	out, err := os.CreateTemp(".", "disk-index-copy-*")
	if err != nil {
		return "", err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return "", err
	}

	return out.Name(), nil
}
