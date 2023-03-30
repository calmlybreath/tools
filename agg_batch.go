package main

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type MGetFn func(ctx context.Context, items []string) (map[string]interface{}, error)
type RoomInfo struct {
	Content string
}
type Task struct {
	TaskId  string           //任务id
	ItemKey string           //item key
	ResCh   chan interface{} //结果channel
	ErrCh   chan error       //错误channel
}

type Config struct {
	ShardingNum int //分片数
	DisChSize   int //分发器channel大小
	//BatchGetFn 批量获取信息
	MGetFn func(ctx context.Context, items []string) (map[string]interface{}, error)
	//定时检查时间
	CheckInterval time.Duration
	MinBatchSize  int //最小批量大小
	MaxBatchSize  int //最大批量大小
}

type Ctx struct {
	context.Context

	cfg Config
}

func NewCtx(ctx context.Context, cfg Config) *Ctx {
	return &Ctx{
		Context: ctx,
		cfg:     cfg,
	}
}

type DispatcherManager struct {
	ctx         *Ctx
	shardingNum int //分片数
	ds          []*Dispatcher
}

func NewDispatcherManager(ctx *Ctx) *DispatcherManager {
	if ctx.cfg.ShardingNum <= 0 {
		panic("shardingNum must > 0")
	}
	if ctx.cfg.CheckInterval <= 0 {
		panic("checkInterval must > 0")
	}
	if ctx.cfg.MinBatchSize <= 0 {
		panic("minBatchSize must > 0")
	}
	if ctx.cfg.MaxBatchSize < ctx.cfg.MinBatchSize {
		panic("maxBatchSize must >= minBatchSize")
	}

	ds := make([]*Dispatcher, ctx.cfg.ShardingNum, ctx.cfg.ShardingNum)
	for i := 0; i < ctx.cfg.ShardingNum; i++ {
		ds[i] = NewDispatcher(
			i, ctx.cfg.MinBatchSize,
			ctx.cfg.MaxBatchSize, ctx.cfg.CheckInterval,
			log.Printf,
			log.Printf,
			ctx.cfg.DisChSize, ctx.cfg.MGetFn)
	}
	return &DispatcherManager{
		shardingNum: ctx.cfg.ShardingNum,
		ds:          ds,
	}
}

func (dm *DispatcherManager) hash() int {
	return rand.Intn(100) % dm.shardingNum
}

func (dm *DispatcherManager) Submit(tasks []Task) {
	dm.ds[dm.hash()].Submit(tasks)
}

type Dispatcher struct {
	Id            int           //id
	MinBatchSize  int           //最小批量大小
	MaxBatchSize  int           //最大批量大小
	CheckInterval time.Duration //定时检查时间
	MGetFn        MGetFn        //批量获取信息方法
	TasksCh       chan []Task   //任务channel

	ErrLogFn   func(string, ...interface{})
	DebugLogFn func(string, ...interface{})

	WaitingTasksQueue *list.List //等待中的任务队列 elem是[]Task
}

func NewDispatcher(
	id int,
	minBatchSize, maxBatchSize int,
	checkInterval time.Duration,
	errLogFn func(string, ...interface{}),
	debugLogFn func(string, ...interface{}),
	tasksChSize int, fn MGetFn) *Dispatcher {
	if debugLogFn == nil {
		debugLogFn = log.Printf
	}
	if errLogFn == nil {
		errLogFn = log.Printf
	}
	dis := &Dispatcher{
		Id:            id,
		MinBatchSize:  minBatchSize,
		MaxBatchSize:  maxBatchSize,
		CheckInterval: checkInterval,
		MGetFn:        fn,
		ErrLogFn:      errLogFn,
		DebugLogFn:    debugLogFn,

		TasksCh:           make(chan []Task, tasksChSize),
		WaitingTasksQueue: list.New(),
	}

	go dis.run()

	return dis
}

func (d *Dispatcher) Submit(tasks []Task) {
	taskLen := len(tasks)
	if taskLen == 0 {
		return
	}
	if taskLen > d.MaxBatchSize {
		d.ErrLogFn("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.MaxBatchSize)
	}
	d.TasksCh <- tasks
}

func (d *Dispatcher) tryGetFetchTasks(mustGteMinBatchSize bool) (map[string][]Task, bool) {
	queueLen := d.WaitingTasksQueue.Len()
	if queueLen == 0 {
		return nil, false
	}
	allElems := make([]*list.Element, 0, queueLen)
	elem := d.WaitingTasksQueue.Front()
	allElems = append(allElems, elem)
	for elem = elem.Next(); elem != nil; elem = elem.Next() {
		allElems = append(allElems, elem)
	}
	gteMinBatchSize := false //是否大于等于最小批量大小
	needFetchElems := make([]*list.Element, 0, queueLen)
	visitedKeys := make(map[string]struct{})
	for _, el := range allElems {
		tasks := el.Value.([]Task)
		for _, t := range tasks {
			visitedKeys[t.ItemKey] = struct{}{}
		}
		curNum := len(visitedKeys)
		if curNum >= d.MinBatchSize {
			gteMinBatchSize = true
			//这里有几种情况
			if curNum > d.MaxBatchSize {
				//那此次遍历elem不能添加进去
				break
			} else {
				needFetchElems = append(needFetchElems, el)
				break
			}
		} else {
			needFetchElems = append(needFetchElems, el)
		}
	}
	if !gteMinBatchSize && mustGteMinBatchSize {
		return nil, false
	}
	if len(needFetchElems) == 0 {
		return nil, false
	}

	itemKey2Tasks := make(map[string][]Task)

	for _, el := range needFetchElems {
		tasks := el.Value.([]Task)
		for _, t := range tasks {
			if _, ok := itemKey2Tasks[t.ItemKey]; !ok {
				itemKey2Tasks[t.ItemKey] = []Task{t}
			} else {
				itemKey2Tasks[t.ItemKey] = append(itemKey2Tasks[t.ItemKey], t)
			}
		}
		d.WaitingTasksQueue.Remove(el)
	}

	return itemKey2Tasks, true
}

// 同一批不会拆分
func (d *Dispatcher) run() {
	ticker := time.NewTicker(d.CheckInterval)
	for {
		select {
		case tasks := <-d.TasksCh:
			d.WaitingTasksQueue.PushBack(tasks)
			//判断下
			key2Tasks, needFetch := d.tryGetFetchTasks(true)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				fmt.Printf("id=%v,fetch tasks by tasksCh\n", d.Id)
				ticker.Reset(d.CheckInterval) //reset
			} else {
			}
		case <-ticker.C:
			key2Tasks, needFetch := d.tryGetFetchTasks(false)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				fmt.Printf("id=%v,fetch tasks by ticker\n", d.Id)
			}
		}
	}
}

func (d *Dispatcher) fetch(ctx context.Context, roomId2Tasks map[string][]Task) {
	roomIds := make([]string, 0, len(roomId2Tasks))
	for roomId := range roomId2Tasks {
		roomIds = append(roomIds, roomId)
	}
	roomId2Info, err := d.MGetFn(ctx, roomIds)
	if err != nil {
		for _, tasks := range roomId2Tasks {
			for _, task := range tasks {
				task.ErrCh <- fmt.Errorf("disId=%v,err=%w", d.Id, err)
				close(task.ErrCh)
			}
		}
		return
	}
	for roomId, tasks := range roomId2Tasks {
		info, ok := roomId2Info[roomId]
		if !ok {
			for _, task := range tasks {
				task.ErrCh <- fmt.Errorf("disId=%v,not found roomid:%s", d.Id, roomId)
				close(task.ErrCh)
			}
			continue
		}
		for _, task := range tasks {
			task.ResCh <- info
			close(task.ResCh)

		}
	}
}

// 底层被dispatch调度，聚合发送请求
// 请求量很小的情况下，可能会增加设定的CheckInterval延迟
func MGetWarp(disManager *DispatcherManager, keys []string) (map[string]interface{}, error) {
	taskList := make([]Task, 0, len(keys))
	for _, roomId := range keys {
		task := Task{
			TaskId:  fmt.Sprintf("%s_%d", roomId, rand.Intn(1000)),
			ItemKey: roomId,
			ResCh:   make(chan interface{}, 1),
			ErrCh:   make(chan error, 1),
		}
		taskList = append(taskList, task)
	}
	disManager.Submit(taskList)

	key2Res := make(map[string]interface{})
	for _, task := range taskList {
		select {
		case res := <-task.ResCh:
			key2Res[task.ItemKey] = res
		case err := <-task.ErrCh:
			return nil, err
		}
	}
	return key2Res, nil
}
