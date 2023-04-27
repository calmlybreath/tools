package agg_batch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type RoomInfo struct {
	Content string
}

func testMGet(ctx context.Context, roomIds []string) (map[string]interface{}, error) {
	roomId2Info := make(map[string]interface{})
	for _, roomId := range roomIds {
		var ct string
		ct = fmt.Sprintf("task_%s", roomId)
		roomId2Info[roomId] = RoomInfo{
			Content: ct,
		}
	}
	//模拟一下延迟
	time.Sleep(time.Millisecond * 500)
	return roomId2Info, nil
}

func TestAggReq(t *testing.T) {
	maxBatchSize := 40
	opts := NewDefaultOpts(testMGet)
	ctx := NewCtx(context.Background(), opts)
	disManager := NewDispatcherManager(ctx)

	wg := sync.WaitGroup{}

	for {
		time.Sleep(1 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			//生成0-maxBatchSize个roomId
			roomNum := rand.Intn(maxBatchSize + 1)
			roomIds := getNRandomRoomId(roomNum)
			id2Res, err := MGetWarp(disManager, roomIds)
			if roomNum > opts.MaxBatchSize {
				if err == nil {
					panic("err must not be nil")
				}
				return
			}
			for _, id := range roomIds {
				if res, ok := id2Res[id]; !ok {
					t.Errorf("BatchGet id=%v,res: %#v", id, res)
				} else {
					roomInfo := res.(RoomInfo)
					expectedRes := fmt.Sprintf("task_%s", id)
					if roomInfo.Content != expectedRes {
						panic(fmt.Sprintf("BatchGet roomIds=%v,res: %#v", roomIds, res))
					}
				}
			}
			t.Logf("succ BatchGet roomIds len=%v,res: %v", len(roomIds), id2Res)
		}()
	}

	wg.Wait()
	fmt.Printf("main exit\n")
}

func getNRandomRoomId(n int) []string {
	roomIds := make([]string, 0)
	for i := 0; i < n; i++ {
		roomId := rand.Intn(10000)
		roomIds = append(roomIds, fmt.Sprintf("room_%d", roomId))
	}
	return roomIds
}

func TestTickerStop(t *testing.T) {
	ticker := time.NewTicker(time.Second * 2)
	after := time.After(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			t.Logf("ticker")
		case <-after:
			ticker.Stop()
			t.Logf("stop ticker")
			ticker = time.NewTicker(time.Second * 2)
		}
	}
}
