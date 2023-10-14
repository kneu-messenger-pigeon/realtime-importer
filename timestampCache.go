package main

import (
	"encoding/binary"
	"github.com/VictoriaMetrics/fastcache"
)

type timeCache struct {
	*fastcache.Cache
}

func (cache *timeCache) Set(id uint, isCustomGroup bool, timestamp int64) {
	cache.Cache.Set(idToBytes(id, isCustomGroup), uintToBytes(uint(timestamp)))
}

func (cache *timeCache) Get(id uint, isCustomGroup bool) int64 {
	timeBytes, exists := cache.Cache.HasGet([]byte{}, idToBytes(id, isCustomGroup))
	if exists {
		return int64(binary.LittleEndian.Uint32(timeBytes))
	}
	return 0
}

func NewTimeCache(maxBytes int) *timeCache {
	return &timeCache{
		Cache: fastcache.New(maxBytes),
	}
}

func idToBytes(id uint, isCustomGroup bool) []byte {
	idBytes := make([]byte, 5)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))

	if isCustomGroup {
		idBytes[4] = 1
	}
	return idBytes
}

func uintToBytes(input uint) []byte {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(input))
	return idBytes
}
