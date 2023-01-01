package main

import (
	"encoding/binary"
	"github.com/VictoriaMetrics/fastcache"
)

type timeCache struct {
	*fastcache.Cache
}

func (cache *timeCache) Set(id uint, timestamp int64) {
	cache.Cache.Set(uintToBytes(id), uintToBytes(uint(timestamp)))
}

func (cache *timeCache) Get(id uint) int64 {
	timeBytes, exists := cache.Cache.HasGet([]byte{}, uintToBytes(id))
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

func uintToBytes(input uint) []byte {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(input))
	return idBytes
}
