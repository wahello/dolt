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
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	makeProfile = false
)

var defaultParams = loadParams{
	cardinality: 100,
	keySize:     20,
	valSize:     5000,
}

// usage: `go test -bench BenchmarkMemoryStore`
func BenchmarkMemoryStore(b *testing.B) {
	benchmarkKVStore(b, newMemStore())
}

// usage: `go test -bench BenchmarkMemProllyStore`
func BenchmarkMemProllyStore(b *testing.B) {
	benchmarkKVStore(b, newMemoryProllyStore())
}

// usage: `go test -bench BenchmarkNBSProllyStore`
func BenchmarkNBSProllyStore(b *testing.B) {
	benchmarkKVStore(b, newNBSProllyStore(os.TempDir()))
}

// usage: `go test -bench BenchmarkBoltStore`
func BenchmarkBoltStore(b *testing.B) {
	benchmarkKVStore(b, newBoltStore(os.TempDir()))
}

// usage: `go test -bench BenchmarkBitcaskStore`
func BenchmarkBitcaskStore(b *testing.B) {
	benchmarkKVStore(b, newBitcaskStore(os.TempDir()))
}

// usage: `go test -bench BenchmarkNBSStore`
func BenchmarkNBSStore(b *testing.B) {
	benchmarkKVStore(b, newNBSStore(os.TempDir()))
}

func benchmarkKVStore(b *testing.B, store keyValStore) {
	//readKeys := loadStore(b, store)
	_ = loadStore(b, store)
	writeKeys, writeVals := generateData(b, defaultParams)

	b.ResetTimer()

	if makeProfile {
		f := makePprofFile(b)
		err := pprof.StartCPUProfile(f)
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			pprof.StopCPUProfile()
			if err = f.Close(); err != nil {
				b.Fatal(err)
			}
			fmt.Printf("\twriting CPU profile for %s: %s\n", b.Name(), f.Name())
		}()
	}

	//b.Run("point reads", func(b *testing.B) {
	//	benchmarkReads(b, store, readKeys)
	//})
	b.Run("batch writes", func(b *testing.B) {
		benchmarkWrites(b, store, writeKeys, writeVals)
	})
}

func loadStore(b *testing.B, store keyValStore) (keys [][]byte) {
	return loadStoreWithParams(b, store, defaultParams)
}

type loadParams struct {
	cardinality uint32
	keySize     uint32
	valSize     uint32
}

func loadStoreWithParams(b *testing.B, store keyValStore, p loadParams) (keys [][]byte) {
	var vals [][]byte
	keys, vals = generateData(b, p)
	store.putMany(keys, vals)
	return
}

func generateData(b *testing.B, p loadParams) (keys, vals [][]byte) {
	keys = make([][]byte, p.cardinality)
	vals = make([][]byte, p.cardinality)

	pairSize := p.keySize + p.valSize
	bufSize := pairSize * p.cardinality

	buf := make([]byte, bufSize)
	_, err := rand.Read(buf)
	require.NoError(b, err)

	for i := range keys {
		off := uint32(i) * pairSize
		keys[i] = buf[off : off+p.keySize]
		vals[i] = buf[off+p.keySize : off+pairSize]
	}
	return
}

func benchmarkReads(b *testing.B, store keyValStore, keys [][]byte) {
	for _, k := range keys {
		v, ok := store.get(k)
		require.NotNil(b, v)
		require.True(b, ok)
	}
}

func benchmarkWrites(b *testing.B, store keyValStore, keys, values [][]byte) {
	batch := 10

	for i := 0; i < len(keys); i += batch {
		lo, hi := i*batch, (i+1)*batch
		if hi > len(keys) {
			break
		}

		kk, vv := keys[lo:hi], values[lo:hi]
		store.putMany(kk, vv)
	}
}

func makePprofFile(b *testing.B) *os.File {
	_, testFile, _, _ := runtime.Caller(0)

	name := fmt.Sprintf("%s_%d.pprof", b.Name(), time.Now().Unix())
	f, err := os.Create(path.Join(path.Dir(testFile), name))
	if err != nil {
		b.Fatal(err)
	}
	return f
}
