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
	"git.mills.io/prologic/bitcask"
)

func newBitcaskStore(path string) keyValStore {
	store, err := bitcask.Open(path)
	if err != nil {
		panic(err)
	}

	return BitcaskStore{store: store}
}

type BitcaskStore struct {
	store *bitcask.Bitcask
}

var _ keyValStore = BitcaskStore{}

func (bit BitcaskStore) get(key []byte) (val []byte, ok bool) {
	var err error
	val, err = bit.store.Get(key)
	if err != nil {
		panic(err)
	}
	ok = val != nil
	return
}

func (bit BitcaskStore) put(key, value []byte) {
	if err := bit.store.Put(key, value); err != nil {
		panic(err)
	}
}

func (bit BitcaskStore) delete(key []byte) {
	if err := bit.store.Delete(key); err != nil {
		panic(err)
	}
}

func (bit BitcaskStore) putMany(keys, values [][]byte) {
	for i := range keys {
		bit.put(keys[i], values[i])
	}
}
