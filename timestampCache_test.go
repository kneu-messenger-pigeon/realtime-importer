package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCacheGetSet(t *testing.T) {
	cache := NewTimeCache(1)

	t.Run("Valid time get-set", func(t *testing.T) {
		id := uint(10)
		expectedValue := time.Now().Unix() - 86400

		cache.Set(id, expectedValue)
		actualValue := cache.Get(id)

		assert.Equal(t, expectedValue, actualValue, "Timestamp value is not expected")
	})

	t.Run("Get not not exist value", func(t *testing.T) {
		id := uint(20)
		expectedValue := int64(0)

		actualValue := cache.Get(id)

		assert.Equal(t, expectedValue, actualValue, "Timestamp value is not expected")
	})
}
