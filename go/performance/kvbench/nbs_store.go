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
	"context"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type NBSStore struct {
	store *nbs.NomsBlockStore
}

var _ keyValStore = NBSStore{}

func newNBSStore(dir string) keyValStore {
	ctx := context.Background()
	verStr := types.Format_Default.VersionString()
	cs, err := nbs.NewLocalStore(ctx, verStr, dir, defaultMemTableSize)
	if err != nil {
		panic(err)
	}

	return NBSStore{
		store: cs,
	}
}

func (nbs NBSStore) get(key []byte) (val []byte, ok bool) {
	ctx := context.Background()
	h := nbs.hashFromKey(key)

	c, err := nbs.store.Get(ctx, h)
	if err != nil {
		panic(err)
	}
	val = c.Data()
	ok = !c.IsEmpty()
	return
}

func (nbs NBSStore) put(key, val []byte) {
	ctx := context.Background()
	h := nbs.putPair(key, val)

	last, err := nbs.store.Root(ctx)
	if err != nil {
		panic(err)
	}

	if _, err = nbs.store.Commit(ctx, h, last); err != nil {
		panic(err)
	}
}

func (nbs NBSStore) putPair(key, val []byte) hash.Hash {
	ctx := context.Background()
	h := nbs.hashFromKey(key)
	c := chunks.NewChunkWithHash(h, val)

	if err := nbs.store.Put(ctx, c); err != nil {
		panic(err)
	}
	return h
}

func (nbs NBSStore) delete(key []byte) {
	panic("unimplemented")
}

func (nbs NBSStore) putMany(keys, vals [][]byte) {
	ctx := context.Background()

	var h hash.Hash
	for i := range keys {
		h = nbs.putPair(keys[i], vals[i])
	}

	last, err := nbs.store.Root(ctx)
	if err != nil {
		panic(err)
	}

	if _, err = nbs.store.Commit(ctx, h, last); err != nil {
		panic(err)
	}
}

func (nbs NBSStore) hashFromKey(key []byte) (h hash.Hash) {
	if len(key) > len(h) {
		key = key[:len(h)]
	}
	copy(h[:], key)
	return
}
