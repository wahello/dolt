// Copyright 2019 Dolthub, Inc.
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

package nbs

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressedChunkIsEmpty(t *testing.T) {
	if !EmptyCompressedChunk.IsEmpty() {
		t.Fatal("EmptyCompressedChunkIsEmpty() should equal true.")
	}
	if !(CompressedChunk{}).IsEmpty() {
		t.Fatal("CompressedChunk{}.IsEmpty() should equal true.")
	}
}

func TestParseTableIndex(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndex(bs)
	require.NoError(t, err)
	defer idx.Close()
	assert.Equal(t, uint32(596), idx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapaddr addr
		e, err := idx.IndexEntry(i, &onheapaddr)
		require.NoError(t, err)
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			lookupe, ok, err := idx.Lookup(&onheapaddr)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, e.Offset(), lookupe.Offset(), "%v does not match %v for address %v", e, lookupe, onheapaddr)
			assert.Equal(t, e.Length(), lookupe.Length())
		}
	}
}

func TestMMapIndex(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndex(bs)
	require.NoError(t, err)
	defer idx.Close()
	tidx, err := newMmapTableIndex(idx, nil)
	require.NoError(t, err)
	defer tidx.Close()

	compareTableIndices(t, tidx, idx)
}

// compareIndices compares a test index (tidx) to a idx
func compareTableIndices(t *testing.T, tidx tableIndex, idx onHeapTableIndex) {
	assert.Equal(t, idx.ChunkCount(), tidx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapAddr addr
		onheapEntry, err := idx.IndexEntry(i, &onheapAddr)
		require.NoError(t, err)
		var tAddr addr
		tentry, err := tidx.IndexEntry(i, &tAddr)
		require.NoError(t, err)
		assert.Equal(t, onheapAddr, tAddr)
		assert.Equal(t, onheapEntry.Offset(), tentry.Offset())
		assert.Equal(t, onheapEntry.Length(), tentry.Length())
		if _, ok := seen[onheapAddr]; !ok {
			seen[onheapAddr] = true
			tentry2, found, err := tidx.Lookup(&onheapAddr)
			require.NoError(t, err)
			assert.True(t, found, "chunkIdx: %d, address: %v", i, onheapAddr)
			if found {
				/* TODO: Check that the chunk at tentry2's offset has the hash onheapAddr
				We can't check that the offset is equivalent here since there may be duplicate chunks in the table file index.
				*/
				//assert.Equal(t, onheapEntry.Offset(), tentry2.Offset(), "chunkIdx: %d, %v does not match %v for address %v", i, onheapEntry, tentry2, onheapAddr)
				assert.Equal(t, onheapEntry.Length(), tentry2.Length())
			}
		}
		wrongaddr := onheapAddr
		if wrongaddr[19] != 0 {
			wrongaddr[19] = 0
			_, found, err := tidx.Lookup(&wrongaddr)
			require.NoError(t, err)
			assert.False(t, found)
		}
	}
	//o1, err := idx.Ordinals()
	//require.NoError(t, err)
	//o2, err := tidx.Ordinals()
	//require.NoError(t, err)
	//assert.Equal(t, o1, o2)
	//p1, err := idx.Prefixes()
	//require.NoError(t, err)
	//p2, err := tidx.Prefixes()
	//require.NoError(t, err)
	//assert.Equal(t, p1, p2)
	//assert.Equal(t, idx.TableFileSize(), tidx.TableFileSize())
	assert.Equal(t, idx.TotalUncompressedData(), tidx.TotalUncompressedData())
}

//// compareIndices compares a test index (tidx) to a idx
//func compareTableIndices(t *testing.T, tidx tableIndex, idx tableIndex) {
//	assert.Equal(t, idx.ChunkCount(), tidx.ChunkCount())
//	seen := make(map[addr]bool)
//	for i := uint32(0); i < idx.ChunkCount(); i++ {
//		var onheapAddr addr
//		onheapEntry, err := idx.IndexEntry(i, &onheapAddr)
//		require.NoError(t, err)
//		var tAddr addr
//		tentry, err := tidx.IndexEntry(i, &tAddr)
//		require.NoError(t, err)
//		require.Equal(t, onheapAddr, tAddr)
//		require.Equal(t, onheapEntry.Offset(), tentry.Offset())
//		require.Equal(t, onheapEntry.Length(), tentry.Length())
//		if _, ok := seen[onheapAddr]; !ok {
//			seen[onheapAddr] = true
//			tentry2, found, err := tidx.Lookup(&onheapAddr)
//			require.NoError(t, err)
//			require.True(t, found)
//			require.Equal(t, onheapEntry.Offset(), tentry2.Offset(), "%v does not match %v for address %v", onheapEntry, tentry2, onheapAddr)
//			require.Equal(t, onheapEntry.Length(), tentry2.Length())
//		}
//		//wrongaddr := onheapAddr
//		//if wrongaddr[19] != 0 {
//		//	wrongaddr[19] = 0
//		//	_, found, err := tidx.Lookup(&wrongaddr)
//		//	require.NoError(t, err)
//		//	require.False(t, found)
//		//}
//	}
//	//o1, err := idx.Ordinals()
//	//require.NoError(t, err)
//	//o2, err := tidx.Ordinals()
//	//require.NoError(t, err)
//	//assert.Equal(t, o1, o2)
//	//p1, err := idx.Prefixes()
//	//require.NoError(t, err)
//	//p2, err := tidx.Prefixes()
//	//require.NoError(t, err)
//	//assert.Equal(t, p1, p2)
//	//assert.Equal(t, idx.TableFileSize(), tidx.TableFileSize())
//	require.Equal(t, idx.TotalUncompressedData(), tidx.TotalUncompressedData())
//}

func TestDiskTableIndex(t *testing.T) {
	f1, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f1.Close()
	bs, err := io.ReadAll(f1)
	require.NoError(t, err)

	idx, err := parseTableIndex(bs)
	require.NoError(t, err)
	defer idx.Close()
	didx, err := NewDiskTableIndexForFile("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.offsets.idx", 0)
	require.NoError(t, err)
	defer didx.Close()

	compareTableIndices(t, didx, idx)
}

func TestCanReadAhead(t *testing.T) {
	type expected struct {
		end uint64
		can bool
	}
	type testCase struct {
		rec       offsetRec
		start     uint64
		end       uint64
		blockSize uint64
		ex        expected
	}
	for _, c := range []testCase{
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 4096, expected{end: 10239, can: true}},
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: false}},
		testCase{offsetRec{offset: 2048, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: true}},
		testCase{offsetRec{offset: (1 << 27), length: 2048}, 0, 128 * 1024 * 1024, 4096, expected{end: 134217728, can: false}},
	} {
		end, can := canReadAhead(c.rec, c.start, c.end, c.blockSize)
		assert.Equal(t, c.ex.end, end)
		assert.Equal(t, c.ex.can, can)
	}
}
