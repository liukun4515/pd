// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	allocStep = uint64(1000)
)

type idAllocator struct {
	mu   sync.Mutex
	base uint64
	end  uint64

	s *Server
}

func (alloc *idAllocator) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	// 如果base也就是已经分配到了end
	if alloc.base == alloc.end {
		// 产生一个新的end
		end, err := alloc.generate()
		if err != nil {
			return 0, errors.Trace(err)
		}

		alloc.end = end
		alloc.base = alloc.end - allocStep
	}

	alloc.base++

	return alloc.base, nil
}

// 向etcd中写一个更大的id，供以后分配
func (alloc *idAllocator) generate() (uint64, error) {
	// 这是id服务存在etcd里面的key
	key := alloc.s.getAllocIDPath()
	value, err := getValue(alloc.s.client, key)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var (
		cmp clientv3.Cmp
		end uint64
	)

	if value == nil {
		// create the key
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		// update the key
		end, err = bytesToUint64(value)
		if err != nil {
			return 0, errors.Trace(err)
		}

		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}
	// 新的end值
	end += allocStep
	value = uint64ToBytes(end)
	resp, err := alloc.s.leaderTxn(cmp).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !resp.Succeeded {
		return 0, errors.New("generate id failed, we may not leader")
	}

	log.Infof("idAllocator allocates a new id: %d", end)
	metadataGauge.WithLabelValues("idalloc").Set(float64(end))
	// 分配一个新的结束id
	return end, nil
}
