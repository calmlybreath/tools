package agg

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/bluele/gcache"
)

func init() {
	go http.ListenAndServe("localhost:6060", nil)
}

// test
func TestAggCache(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	config := NewDefaultAggConfig()
	agg, err := NewAgg(ctx, BuildAggParams{
		Config: config,
		MGetFn: testMGet,
	})
	if err != nil {
		t.Fatal(err)
	}
	normalCache := gcache.New(10000).LFU().Build()
	aggCache := NewSFAggCache(agg, normalCache, nil, nil, time.Millisecond*300, true)

	for {
		time.Sleep(GenGoInterval)
		go func() {
			roomNum := rand.Intn(30 + 1)
			keys := GenRoomIdN(roomNum)
			k2res, err := aggCache.MGet(ctx, keys)
			if err != nil {
				panic(err)
			}
			for _, key := range keys {
				if _, ok := k2res[key]; !ok {
					panic(fmt.Sprintf("key not found=%v", key))
				}
				res := k2res[key].((RoomInfo))
				if res != GetRes(key.(string)) {
					panic(fmt.Sprintf("key:%v res:%v not equal", key, res))
				}
				//t.Logf("key:%v res:%v",key,res)
			}
		}()
	}

}

func TestSimplestAggCache(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	aggCache, err := NewSFAggCacheV3(ctx, &SimpleRoomInfoMGet{})
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(GenGoInterval)
		go func() {
			roomNum := rand.Intn(30 + 1)
			keys := GenRoomIdN(roomNum)
			k2res, err := aggCache.MGet(ctx, keys)
			if err != nil {
				panic(err)
			}
			for _, key := range keys {
				if _, ok := k2res[key]; !ok {
					panic(fmt.Sprintf("key not found=%v", key))
				}
				res := k2res[key].((RoomInfo))
				if res != GetRes(key.(string)) {
					panic(fmt.Sprintf("key:%v res:%v not equal", key, res))
				}
				//t.Logf("key:%v res:%v",key,res)
			}
		}()
	}
}

func TestAggCacheV1(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	aggCache, err := NewSFAggCacheV1(ctx, &SimpleRoomInfoMGet{}, 10000, time.Millisecond*10000)
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(GenGoInterval)
		go func() {
			roomNum := rand.Intn(30 + 1)
			keys := GenRoomIdN(roomNum)
			k2res, err := aggCache.MGet(ctx, keys)
			if err != nil {
				panic(err)
			}
			for _, key := range keys {
				if _, ok := k2res[key]; !ok {
					panic(fmt.Sprintf("key not found=%v", key))
				}
				res := k2res[key].((RoomInfo))
				if res != GetRes(key.(string)) {
					panic(fmt.Sprintf("key:%v res:%v not equal", key, res))
				}
				//t.Logf("key:%v res:%v",key,res)
			}
		}()
	}
}

type SimpleRoomInfoMGet struct{}

func (this *SimpleRoomInfoMGet) MGet(ctx context.Context, keys []interface{}) (map[interface{}]interface{}, error) {
	return testMGet(ctx, keys)
}
