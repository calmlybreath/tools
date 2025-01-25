package agg

import (
	"fmt"
	"testing"
	"time"

	"github.com/bluele/gcache"
)

func TestSFCache(t *testing.T) {
	sfc := NewSFCache(gcache.New(10000).LFU().Build(), time.Second)
	go func() {
		time.Sleep(time.Minute)
		stat := sfc.StatAndClear()
		t.Logf("stat:%+v", stat)
	}()
	for {
		time.Sleep(time.Microsecond * 10)
		go func() {
			key := GenRoomIdN(1)[0]
			dynValue, _ := sfc.Get(key, func() (interface{}, error) {
				time.Sleep(time.Millisecond * 200)
				return fmt.Sprintf("res_%v", key), nil
			})
			v := dynValue.(string)
			kstr := key.(string)

			if v != fmt.Sprintf("res_%v", kstr) {
				panic(fmt.Sprintf("error:%v", v))
			}
			t.Logf("key:%v,value:%v", key, dynValue)
		}()
	}
}
