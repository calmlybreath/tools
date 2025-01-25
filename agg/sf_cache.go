package agg

import (
	"time"
)

type SFCache struct {
	singleflight Singleflight
	cache        ICache
	expiration   time.Duration
}

// new
func NewSFCache(cache ICache, expiration time.Duration) *SFCache {
	if expiration == 0 {
		panic("expiration must > 0")
	}
	return &SFCache{
		cache:        cache,
		singleflight: Singleflight{},
		expiration:   expiration,
	}
}

func (this *SFCache) Get(key interface{}, fn func() (interface{}, error)) (interface{}, error) {
	cachedValue, err := this.cache.Get(key)
	if err == nil {
		return cachedValue, nil
	}
	value, err, _ :=
		this.singleflight.Do(key, func() (interface{}, error) {
			v, err := fn()
			if err != nil {
				return nil, err
			}
			this.cache.SetWithExpire(key, v, this.expiration)
			return v, nil
		},
		)
	return value, err
}

type SFCacheStat struct {
	SingleflightStat
	CacheHitRate float64
}

func (this *SFCache) StatAndClear() SFCacheStat {
	sfStat := this.singleflight.StatAndClear()
	cacheHitRate := this.cache.HitRate()
	return SFCacheStat{
		SingleflightStat: sfStat,
		CacheHitRate:     cacheHitRate,
	}
}
