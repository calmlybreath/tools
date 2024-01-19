package aggbatch

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const FetchTimeoutMs = 100
const RealFetchTimeoutMs = 50 * time.Millisecond
const GoGenerateInterval = time.Microsecond * 1000
const AggRunTime = time.Second * 3000 //多少秒后cancel

func TestAggReq(t *testing.T) {
	t.Logf("start TestAggReq")
	ctx, cancel := context.WithCancel(context.Background())

	aggCfg := NewAggCfg()
	aggCfg.MGetTimeoutMs = FetchTimeoutMs
	aggCfg.MGetFn = testMGet
	mngCfg := NewAggMngCfg("test")
	mngCfg.AggNum = 1
	disManager := NewAggManager(ctx, mngCfg, aggCfg)

	go func() {
		time.Sleep(AggRunTime)
		t.Logf("cancel TestAggReq")
		cancel()
	}()

	wg := sync.WaitGroup{}
	for {
		time.Sleep(GoGenerateInterval)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//生成0-maxBatchSize个roomId
			roomNum := rand.Intn(aggCfg.MaxBatchSize + 1)
			roomIds := getNRandomRoomId(roomNum)
			id2Res, err := MGetWarp(ctx, disManager, roomIds)
			if err != nil {
				if errors.Is(AggMngClosedError, err) {
					panic(err)
				}
				t.Errorf("BatchGet roomIds=%v,err: %v", roomIds, err)
				return
			}
			for _, id := range roomIds {
				if res, ok := id2Res[id]; !ok {
					t.Errorf("BatchGet id=%v,res: %#v", id, res)
				} else {
					roomInfo := res.(RoomInfo)
					realRoomInfo := GetRes(id.(string))
					if roomInfo.Content != realRoomInfo.Content {
						panic(fmt.Sprintf("BatchGet roomIds=%v,res: %#v", roomIds, res))
					}
					t.Logf("BatchGet roomId=%v,res: %#v", id, roomInfo)
				}
			}
			t.Logf("succ BatchGet roomIds len=%v,res: %v", len(roomIds), id2Res)
		}()
	}

}

func GetRes(roomId string) RoomInfo {
	return RoomInfo{
		Content: fmt.Sprintf("task_%s", roomId),
	}
}

func getNRandomRoomId(n int) []interface{} {
	roomIds := make([]interface{}, 0)
	for i := 0; i < n; i++ {
		roomId := rand.Intn(10000000000)
		roomIds = append(roomIds, fmt.Sprintf("room_%d", roomId))
	}
	return roomIds
}

type RoomInfo struct {
	Content string
}

func testMGet(ctx context.Context, meteData map[string]interface{}, roomIds []interface{}) (map[interface{}]interface{}, error) {
	roomId2Info := make(map[interface{}]interface{})
	for _, roomId := range roomIds {
		roomId2Info[roomId] = GetRes(roomId.(string))
	}
	//模拟一下延迟
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(RealFetchTimeoutMs):
			return roomId2Info, nil
		}
	}
}
