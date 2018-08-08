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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	clusterPath  = "raft"
	configPath   = "config"
	schedulePath = "schedule"
	gcPath       = "gc"
)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// KV wraps all kv operations, keep it stateless.
type KV struct {
	KVBase
}

// NewKV creates KV instance with KVBase.
// kv只是对外提供服务的抽象
// 底层的kv base才是真正对数据操作的
// 这样的做法可以把两个内容进行分离

// 直接供给给外部server使用
// KV封装了所有需要些数据的操作的内容
func NewKV(base KVBase) *KV {
	return &KV{
		KVBase: base,
	}
}

// store元信息对应的编码key
// clusterPath+s+id
func (kv *KV) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

// region元信息对应的编码key
// clusterPath+r+id
func (kv *KV) regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
// status信息
// clusterPath+status+option
func (kv *KV) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

// ？？？什么意思
func (kv *KV) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

// ？？？什么意思
func (kv *KV) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// LoadMeta loads cluster meta from KV store.
func (kv *KV) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(clusterPath, meta)
}

// SaveMeta save cluster meta to KV store.
func (kv *KV) SaveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(clusterPath, meta)
}

// LoadStore loads one store from KV.
func (kv *KV) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return kv.loadProto(kv.storePath(storeID), store)
}

// SaveStore saves one store to KV.
func (kv *KV) SaveStore(store *metapb.Store) error {
	return kv.saveProto(kv.storePath(store.GetId()), store)
}

// LoadRegion loads one regoin from KV.
func (kv *KV) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.loadProto(kv.regionPath(regionID), region)
}

// SaveRegion saves one region to KV.
func (kv *KV) SaveRegion(region *metapb.Region) error {
	return kv.saveProto(kv.regionPath(region.GetId()), region)
}

// DeleteRegion deletes one region from KV.
func (kv *KV) DeleteRegion(region *metapb.Region) error {
	return kv.Delete(kv.regionPath(region.GetId()))
}

// SaveConfig stores marshalable cfg to the configPath.
func (kv *KV) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (kv *KV) LoadConfig(cfg interface{}) (bool, error) {
	value, err := kv.Load(configPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// LoadStores loads all stores from KV to StoresInfo.
// load 所有的store信息
func (kv *KV) LoadStores(stores *StoresInfo) error {
	nextID := uint64(0)
	// end key是64的最大值
	endKey := kv.storePath(math.MaxUint64)
	for {
		key := kv.storePath(nextID)
		res, err := kv.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, s := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(s)); err != nil {
				return errors.Trace(err)
			}
			storeInfo := NewStoreInfo(store)
			leaderWeight, err := kv.loadFloatWithDefaultValue(kv.storeLeaderWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.LeaderWeight = leaderWeight
			regionWeight, err := kv.loadFloatWithDefaultValue(kv.storeRegionWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.RegionWeight = regionWeight

			nextID = store.GetId() + 1
			stores.SetStore(storeInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

// SaveStoreWeight saves a store's leader and region weight to KV.
func (kv *KV) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := kv.Save(kv.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return errors.Trace(err)
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	if err := kv.Save(kv.storeRegionWeightPath(storeID), regionValue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *KV) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := kv.Load(path)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *KV) LoadRegions(regions *RegionsInfo) error {
	nextID := uint64(0)
	endKey := kv.regionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit

	for {
		key := kv.regionPath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
				continue
			}
			return errors.Trace(err)
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			overlaps := regions.SetRegion(NewRegionInfo(region, nil))
			for _, item := range overlaps {
				if err := kv.DeleteRegion(item); err != nil {
					return errors.Trace(err)
				}
			}
		}

		if len(res) < int(rangeLimit) {
			return nil
		}
	}
}

// 从真正实现kv的结构体中load数据，数据的结果存在msg中
// SaveGCSafePoint saves new GC safe point to KV.
func (kv *KV) SaveGCSafePoint(safePoint uint64) error {
	key := path.Join(gcPath, "safe_point")
	value := strconv.FormatUint(safePoint, 16)
	if err := kv.Save(key, value); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadGCSafePoint loads current GC safe point from KV.
func (kv *KV) LoadGCSafePoint() (uint64, error) {
	key := path.Join(gcPath, "safe_point")
	value, err := kv.Load(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if value == "" {
		return 0, nil
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}


// kv中最基本的操作

// 把key，value中的数据进行序列化或者反序列

// 存储到kv base中或者从kv base中读取出来
func (kv *KV) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.Load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	return true, proto.Unmarshal([]byte(value), msg)
}

// 从真正实现kv的结构总save数据，数据的内容存在在msg中
// kv中最基本的操作，
func (kv *KV) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.Save(key, string(value))
}
