package main

import (
	"github.com/VictoriaMetrics/fastcache"
)

type countCache struct {
	*fastcache.Cache
}

func (cache *countCache) Set(key *string, count uint8) {
	cache.Cache.Set([]byte(*key), []byte{count})
}

func (cache *countCache) Get(key *string) uint8 {
	countBytes, exists := cache.Cache.HasGet([]byte{}, []byte(*key))
	if exists {
		return countBytes[0]
	}
	return 0
}

func NewCountCache(maxBytes int) *countCache {
	return &countCache{
		Cache: fastcache.New(maxBytes),
	}
}
