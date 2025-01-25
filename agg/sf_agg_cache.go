package agg

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluele/gcache"
	"golang.org/x/sync/errgroup"
)

type SFAggCacheStat struct {
	CallMGetNum uint64
}

// 为了不增加对下游批量查询的qps的情况下，使用cache、singleflight
// 对于批量查询，可以cache、防止击穿
type SFAggCache struct {
	aggImpl      IAgg
	singleflight Singleflight
	stat         SFAggCacheStat

	normalCache       ICache
	importantKeyImpl  IImportantKey
	importantKeyCache ICache

	cacheExpiration      time.Duration
	ignoreKeyResNotExist bool
}

// 最基本,只有防止击穿的效果
// 使用了默认的agg、不使用了cache
func NewSFAggCacheV3(ctx context.Context, mgetImpl IMGet) (*SFAggCache, error) {
	aggCfg := NewDefaultAggConfig()
	agg, err := NewAgg(ctx, BuildAggParams{
		Id:       "defaultAgg",
		Config:   aggCfg,
		MGetImpl: mgetImpl,
		ErrorLog: log.Printf,
	})
	if err != nil {
		return nil, err
	}
	return &SFAggCache{
		aggImpl:      agg,
		singleflight: Singleflight{},
	}, nil
}

// 最常用的
// 使用了lfu cache,大小为10000
// 可以防止击穿+使用缓存
func NewSFAggCacheV1(ctx context.Context, mgetImpl IMGet, cacheSize int, cacheExpiration time.Duration) (*SFAggCache, error) {
	aggCfg := NewDefaultAggConfig()
	agg, err := NewAgg(ctx, BuildAggParams{
		Id:       "defaultAgg",
		Config:   aggCfg,
		MGetImpl: mgetImpl,
		ErrorLog: log.Printf,
	})
	if err != nil {
		return nil, err
	}
	normalCache := gcache.New(cacheSize).LFU().Build()
	return NewSFAggCache(
		agg,
		normalCache,
		nil,
		nil,
		cacheExpiration,
		true,
	), nil
}

// 大房间空降模式使用
// 对于重要的key，会使用独立的缓存,需要实现importantKeyImpl
// 使用了lfu cache
// 可以防止击穿+缓存+保证大房间缓存不会被淘汰
func NewSFAggCacheV2(
	ctx context.Context,
	mgetImpl IMGet,
	useNormalCache bool,
	importantKeyImpl IImportantKey,
	cacheSize int,
	cacheExpiration time.Duration) (*SFAggCache, error) {
	aggCfg := NewDefaultAggConfig()
	agg, err := NewAgg(ctx, BuildAggParams{
		Id:       "defaultAgg",
		Config:   aggCfg,
		MGetImpl: mgetImpl,
		ErrorLog: log.Printf,
	})
	if err != nil {
		return nil, err
	}

	return NewSFAggCache(
		agg,
		gcache.New(cacheSize).LFU().Build(),
		importantKeyImpl,
		gcache.New(cacheSize).LFU().Build(),
		cacheExpiration,
		true,
	), nil
}

func NewSFAggCache(
	aggImpl IAgg,
	normalCache ICache,
	importantKeyImpl IImportantKey,
	importantKeyCache ICache,
	cacheExpiration time.Duration,
	ignoreKeyResNotExist bool,
) *SFAggCache {

	return &SFAggCache{
		aggImpl:              aggImpl,
		normalCache:          normalCache,
		importantKeyImpl:     importantKeyImpl,
		importantKeyCache:    importantKeyCache,
		cacheExpiration:      cacheExpiration,
		singleflight:         Singleflight{},
		ignoreKeyResNotExist: ignoreKeyResNotExist,
	}
}

type ItemNotExistError struct {
	Key interface{}
}

// impl error
func (this *ItemNotExistError) Error() string {
	return fmt.Sprintf("key not exist:%s", this.Key)
}

type DisplaySFAggCacheStat struct {
	CallAggCacheMGetNum uint64
	SingleflightStat    SingleflightStat

	ImportCacheHitRate float64
	NormalCacheHitRate float64

	AggStat DisplayAggStat
}

/*
func (this *SFAggCache) StatAndClear() DisplaySFAggCacheStat {
	singleflightStat := this.singleflight.StatAndClear()
	aggStat := this.aggImpl.StatAndClear()
	callAggCacheMGetNum := atomic.LoadUint64(&this.stat.CallMGetNum)
	atomic.StoreUint64(&this.stat.CallMGetNum, 0)

	return DisplaySFAggCacheStat{
		CallAggCacheMGetNum: callAggCacheMGetNum,
		SingleflightStat:    singleflightStat,
		ImportCacheHitRate: func() float64 {
			if this.importantKeyCache != nil {
				return this.importantKeyCache.HitRate()
			}
			return 0
		}(),
		NormalCacheHitRate: func() float64 {
			if this.normalCache != nil {
				return this.normalCache.HitRate()
			}
			return 0
		}(),
		AggStat: aggStat,
	}
}
*/

func (this *SFAggCache) useImportCache() bool {
	return this.importantKeyImpl != nil && this.importantKeyCache != nil
}

func (this *SFAggCache) MGet(ctx context.Context, keys []interface{}) (map[interface{}]interface{}, error) {
	atomic.AddUint64(&this.stat.CallMGetNum, 1)
	key2Res := make(map[interface{}]interface{})
	for _, key := range keys {
		if this.useImportCache() && this.importantKeyImpl.IsImportant(ctx, key) {
			if val, err := this.importantKeyCache.Get(key); err == nil {
				key2Res[key] = val
			}
		} else {
			if this.normalCache != nil {
				if val, err := this.normalCache.Get(key); err == nil {
					key2Res[key] = val
				}
			}
		}
	}

	if len(key2Res) == len(keys) {
		return key2Res, nil
	}

	missedKeys := make([]interface{}, 0)
	for _, key := range keys {
		if _, ok := key2Res[key]; !ok {
			missedKeys = append(missedKeys, key)
		}
	}

	missedKey2Res := sync.Map{}
	wg, _ := errgroup.WithContext(ctx)
	for _, tempKey := range missedKeys {
		key := tempKey
		wg.Go(func() error {
			res, err, _ := this.singleflight.Do(key, func() (interface{}, error) {
				key2Res, err := this.aggImpl.SubmitAndWait(ctx, []interface{}{key})
				if err != nil {
					return nil, fmt.Errorf("agg.SubmitAndWait failed: key(%s), err(%w)", key, err)
				}
				res, ok := key2Res[key]
				if !ok {
					return nil, &ItemNotExistError{Key: key}
				}
				if this.useImportCache() && this.importantKeyImpl.IsImportant(ctx, key) {
					this.importantKeyCache.SetWithExpire(key, res, this.cacheExpiration)
				} else if this.normalCache != nil {
					this.normalCache.SetWithExpire(key, res, this.cacheExpiration)
				}
				return res, nil
			})
			if err != nil {
				if _, ok := err.(*ItemNotExistError); ok && this.ignoreKeyResNotExist {
					return nil
				}
				return err
			}
			missedKey2Res.Store(key, res)
			return nil
		})
	}
	err := wg.Wait()
	if err != nil {
		return nil, fmt.Errorf("agg.SubmitAndWait failed: err(%w)", err)
	}

	missedKey2Res.Range(func(key, value interface{}) bool {
		key2Res[key] = value
		return true
	})
	return key2Res, nil
}
