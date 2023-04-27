package mem_cache

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/bluele/gcache"
	"golang.org/x/sync/singleflight"
)




type CommonCache struct {
	innerCache ICache
	sgflight   singleflight.Group
	expiration time.Duration //缓存有效时间
}

func NewCommonCache(c ICache, expiration time.Duration) *CommonCache {
	return &CommonCache{
		innerCache: c,
		expiration: expiration,
		sgflight:   singleflight.Group{},
	}
}

//默认使用singleFlight
func (c *CommonCache) DefaultGet(key string, fn func() (interface{}, error)) (interface{}, error) {
	item, exist := c.innerCache.Get(key)
	if exist {
		return item, nil
	}
	item1, err, _ := c.sgflight.Do(key, func() (interface{}, error) {
		item1, err := fn()
		if err != nil {
			return nil, err
		}
		c.innerCache.Set(key, item1, c.expiration)
		return item1, nil
	})
	if err != nil {
		return nil, err
	}
	return item1, nil
}

type CacheItem struct {
	Key string
	Val interface{}
}
type ICache interface {
	Set(key string, val interface{}, expired time.Duration)
	Get(key string) (interface{}, bool)
}

type GroupCacheStat struct {
	LastCleanExpiredCost time.Duration //上次清理过期key的耗时
}
type IntervalRunFunc func(groupCache *GroupCache)
type GroupCache struct {
	cacheType             string         //缓存类型
	shards                []gcache.Cache //分片
	shardNum              int            //分片数量
	eachShardSize         int            //每个分片的大小
	stat                  GroupCacheStat
	customIntervalRunFunc IntervalRunFunc //定时执行的函数
	runCustomInterval     time.Duration   //定时执行的时间间隔
	cleanExpiredInterval  time.Duration   //需要定时清理过期key间隔
	debugLogFn            func(format string, args ...interface{})
	errorLogFn            func(format string, args ...interface{})
}
type GroupCacheOptions struct{
	CacheType string //缓存类型
	ShardNum int //分片数量
	EachShardingSize int //每个分片的大小
	CustomIntervalRunFunc IntervalRunFunc  //用户自己定义的定时执行的函数
	RunCustomInterval time.Duration //定时执行的时间间隔
	CleanExpiredInterval time.Duration //定时清理过期key,如果是0则不清理
	DebugLogFn func(format string, args ...interface{})
	ErrorLogFn func(format string, args ...interface{})
}


//iter
func (g *GroupCache) Iterator() []CacheItem {
	var items []CacheItem
	for i := 0; i < g.shardNum; i++ {
		all := g.shards[i].GetALL(true)
		for k, v := range all {
			items = append(items, CacheItem{
				Key: k.(string),
				Val: v,
			})
		}
	}
	return items
}

func (g *GroupCache) GetShardingCacheLen(num int) (total int, notExpiredNum int) {
	if num >= g.shardNum {
		return 0, 0
	}
	return g.shards[num].Len(false), g.shards[num].Len(true)
}

func (g *GroupCache) GetSharingNum() int {
	return g.shardNum
}

func NewGroupCache(
	ctx context.Context,
	opts GroupCacheOptions) ICache {
	if opts.ShardNum==0{
		opts.ShardNum=1
	}
	shards := make([]gcache.Cache, opts.ShardNum)
	switch opts.CacheType {
	case gcache.TYPE_LRU:
		for i := 0; i < opts.ShardNum; i++ {
			shards[i] = gcache.New(opts.EachShardingSize).LFU().Build()
		}
	case gcache.TYPE_LFU:
		for i := 0; i < opts.ShardNum; i++ {
			shards[i] = gcache.New(opts.EachShardingSize).LFU().Build()
		}
	case gcache.TYPE_ARC:
		for i := 0; i < opts.ShardNum; i++ {
			shards[i] = gcache.New(opts.EachShardingSize).ARC().Build()
		}
	}

	gc:= &GroupCache{
		cacheType: opts.CacheType,
		shards:   shards,
		shardNum: opts.ShardNum,
		eachShardSize: opts.EachShardingSize,
		customIntervalRunFunc: opts.CustomIntervalRunFunc,
		runCustomInterval: opts.RunCustomInterval,
		cleanExpiredInterval: opts.CleanExpiredInterval,
		debugLogFn: opts.DebugLogFn,
		errorLogFn: opts.ErrorLogFn,
	}
	go gc.run(ctx)
	return gc
}

func (g *GroupCache) run(ctx context.Context){
	if g.customIntervalRunFunc==nil && g.cleanExpiredInterval==0{
		return
	}
	if g.runCustomInterval == 0{
		g.runCustomInterval = time.Second * 60
	}
	customTicker := time.NewTicker(g.runCustomInterval)
	cleanTicker:=time.NewTicker(g.cleanExpiredInterval)
	for {
		select {
		case <-customTicker.C:
			g.customIntervalRunFunc(g)
		case <-cleanTicker.C:
			g.cleanExpired()
		case <-ctx.Done():
			return
		}
	}
}

func (g *GroupCache) cleanExpired(){
	//todo
}

//set
func (g *GroupCache) Set(key string, val interface{}, expiration time.Duration) {
	ksum:=g.key2Num(key)
	index:=ksum %g.shardNum
	if g.debugLogFn!=nil{
		g.debugLogFn("key=%v,ksum=%v,index=%v,shardingNum=%v",key,ksum,index,g.shardNum)
	}
	shard := g.shards[index]
	shard.SetWithExpire(key, val, expiration)
}

//hash string to int

//get
func (g *GroupCache) Get(key string) (interface{}, bool) {
	shard := g.shards[g.key2Num(key)%g.shardNum]
	val, err := shard.Get(key)
	if err != nil {
		return nil, false
	}
	return val, true
}

func (g *GroupCache) key2Num(key string) int {
	return Key2Num(key)
}

//key2Num
func Key2Num(key string) int {
	h := fnv.New32a()
	bs := []byte(key)
	h.Write(bs)
	return int(h.Sum32())
}





