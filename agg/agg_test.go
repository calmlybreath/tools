package agg

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const MGetTimeoutMs = 200
const RealFetchTimeoutMs = time.Millisecond
const GenGoInterval = time.Microsecond * 1000
const AggRunTime = time.Second * 30000 //多少秒后cancel

func TestAggReq(t *testing.T) {
	t.Logf("start TestAggReq")
	ctx, cancel := context.WithCancel(context.Background())

	cfg := NewDefaultAggConfig()
	cfg.MGetTimeoutMs = MGetTimeoutMs
	cfg.MaxWaitMs = 1
	agg, err := NewAgg(ctx, BuildAggParams{
		Id:       "testAgg",
		Config:   cfg,
		MGetFn:   testMGet,
		MGetImpl: nil,
	})
	if err != nil {
		t.Fatalf("NewAgg err:%v", err)
	}

	go func() {
		time.Sleep(AggRunTime)
		cancel()
	}()

	for {
		time.Sleep(GenGoInterval)
		go func() {
			//生成0-maxBatchSize个roomId
			roomNum := rand.Intn(cfg.MaxBatchSize + 1)
			roomIds := getNRandomRoomId(roomNum)
			id2Res, err := agg.SubmitAndWait(ctx, roomIds)
			if err != nil {
				if errors.Is(AggClosedError, err) {
					time.Sleep(time.Second)
					panic(err)
				}
				t.Logf("BatchGet roomIds=%v,err: %v", roomIds, err)
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
		roomId := rand.Intn(10000)
		roomIds = append(roomIds, fmt.Sprintf("room_%d", roomId))
	}
	return roomIds
}

func GenRoomIdN(n int) []interface{} {
	roomIds := make([]interface{}, 0)
	for i := 0; i < n; i++ {
		roomId := rand.Intn(10000)
		roomIds = append(roomIds, fmt.Sprintf("room_%d", roomId))
	}
	return roomIds
}

type RoomInfo struct {
	Content string
}

func testMGet(ctx context.Context, roomIds []interface{}) (map[interface{}]interface{}, error) {
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
