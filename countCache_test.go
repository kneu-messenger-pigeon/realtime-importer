package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCountCacheGetSet(t *testing.T) {
	cache := NewCountCache(1)

	t.Run("Valid time get-set", func(t *testing.T) {
		key := "key-1"
		expectedCount := uint8(6)

		cache.Set(&key, expectedCount)
		acualCount := cache.Get(&key)

		assert.Equal(t, expectedCount, acualCount, "Timestamp value is not expected")
	})

	t.Run("Get not not exist value", func(t *testing.T) {
		key := "key-20"
		expectedValue := uint8(0)

		actualValue := cache.Get(&key)

		assert.Equal(t, expectedValue, actualValue, "Timestamp value is not expected")
	})
}
