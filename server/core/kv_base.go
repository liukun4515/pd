// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"sync"

	"github.com/google/btree"
)

// KVBase is an abstract interface for load/save pd cluster data.
// 基本的kv操作，两种实现，mem和etcd
type KVBase interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) ([]string, error)
	Save(key, value string) error
	Delete(key string) error
}

type memoryKV struct {
	sync.RWMutex
	// 使用btree来实现
	tree *btree.BTree
}

// NewMemoryKV returns an in-memory kvBase for testing.
func NewMemoryKV() KVBase {
	return &memoryKV{
		tree: btree.New(2),
	}
}

type memoryKVItem struct {
	key, value string
}

// item就是要实现比较函数
func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

// 没有内容的时候是""
func (kv *memoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	// btree中获取某个key
	item := kv.tree.Get(memoryKVItem{key, ""})
	if item == nil {
		return "", nil
	}
	return item.(memoryKVItem).value, nil
}

func (kv *memoryKV) LoadRange(key, endKey string, limit int) ([]string, error) {
	kv.RLock()
	defer kv.RUnlock()
	res := make([]string, 0, limit)
	// 增序查找一个range
	// 每次追加到res中，如果返回的是false就继续操作
	// 直到返回true的时候才停止
	kv.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item btree.Item) bool {
		res = append(res, item.(memoryKVItem).value)
		return len(res) < int(limit)
	})
	return res, nil
}

func (kv *memoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	// 内存插入一个节点的信息
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

func (kv *memoryKV) Delete(key string) error {
	kv.Lock()
	defer kv.Unlock()
	// 内存中delete一个节点的信息
	kv.tree.Delete(memoryKVItem{key, ""})
	return nil
}
