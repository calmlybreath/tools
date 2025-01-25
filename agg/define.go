package agg

import (
	"context"
	"time"

	"github.com/bluele/gcache"
)

type IAgg interface {
	SubmitAndWait(ctx context.Context, items []interface{}) (item2Res map[interface{}]interface{}, err error)
}

type ICache interface {
	SetWithExpire(key, value interface{}, expiration time.Duration) error
	Get(key interface{}) (interface{}, error)
	HitRate() float64
}

type IImportantKey interface {
	IsImportant(ctx context.Context, key interface{}) bool
}

type ImportantStringKeyImpl struct {
	keys map[string]struct{}
}

// new
func NewImportStringKeyImpl(keys []string) *ImportantStringKeyImpl {
	km := make(map[string]struct{})
	for _, k := range keys {
		km[k] = struct{}{}
	}
	return &ImportantStringKeyImpl{
		keys: km,
	}
}
func (this *ImportantStringKeyImpl) IsImportant(ctx context.Context, dynString interface{}) bool {
	if _, ok := this.keys[dynString.(string)]; ok {
		return true
	}
	return false
}

type Cache struct {
	gcache.Cache
}

// new lfu
func NewLFUCache(size int) *Cache {
	return &Cache{
		Cache: gcache.New(size).LFU().Build(),
	}
}

// new lru
func NewLRUCache(size int) *Cache {
	return &Cache{
		Cache: gcache.New(size).LRU().Build(),
	}
}
