package agg_batch

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)




type BatchGetFn func(ctx context.Context, items []string) (map[string]interface{}, error)

type Task struct {
	TaskId  string           //任务id
	ItemKey string           //item key
	ResCh   chan interface{} //结果channel
	ErrCh   chan error       //错误channel
}

type Options struct {
	ShardingNum           int           //分片数
	DisChSize             int           //分发器channel大小
	CheckInterval         time.Duration //检查间隔
	MinBatchSize          int           //最小批量大小
	MaxBatchSize          int           //最大批量大小
	IgnoreItemResNotExist bool          //忽略item查询结果不存在

	BatchGetFn BatchGetFn //BatchGetFn 批量获取信息
	ErrLogFn   func(format string, v ...interface{})
	DebugLogFn func(format string, v ...interface{})
}

type Ctx struct {
	context.Context
	Opts Options
}

func NewDefaultOpts(fn BatchGetFn) Options {
	return Options{
		ShardingNum:           10,
		DisChSize:             500,
		CheckInterval:         5 * time.Millisecond,
		MinBatchSize:          20,
		MaxBatchSize:          30,
		BatchGetFn:            fn,
		IgnoreItemResNotExist: true,
		ErrLogFn:              log.Printf,
		DebugLogFn:            log.Printf,
	}
}

func NewCtx(ctx context.Context, opts Options) *Ctx {
	return &Ctx{
		Context: ctx,
		Opts:    opts,
	}
}

type DispatcherManager struct {
	ctx         *Ctx
	shardingNum int //分片数
	ds          []*Dispatcher
}

func NewDispatcherManager(ctx *Ctx) *DispatcherManager {
	if ctx.Opts.ShardingNum <= 0 {
		panic("shardingNum must > 0")
	}
	if ctx.Opts.MinBatchSize <= 0 {
		panic("minBatchSize must > 0")
	}
	if ctx.Opts.MaxBatchSize < ctx.Opts.MinBatchSize {
		panic("maxBatchSize must >= minBatchSize")
	}
	if ctx.Opts.ErrLogFn == nil {
		ctx.Opts.ErrLogFn = log.Printf
	}
	if ctx.Opts.DebugLogFn == nil {
		ctx.Opts.DebugLogFn = log.Printf
	}

	ds := make([]*Dispatcher, ctx.Opts.ShardingNum, ctx.Opts.ShardingNum)
	for i := 0; i < ctx.Opts.ShardingNum; i++ {
		ds[i] = NewDispatcher(i, ctx)
	}
	return &DispatcherManager{
		shardingNum: ctx.Opts.ShardingNum,
		ds:          ds,
	}
}

func (dm *DispatcherManager) hash() int {
	return rand.Intn(100) % dm.shardingNum
}

func (dm *DispatcherManager) Submit(tasks []Task) error {
	return dm.ds[dm.hash()].Submit(tasks)
}

type TaskChunk struct {
	SubmitTime time.Time //提交时间
	Tasks      []Task
}
type Dispatcher struct {
	ctx context.Context
	Id  int //id
	Options
	TaskChunkCh       chan TaskChunk //任务channel
	WaitingTasksQueue *list.List     //等待中的任务队列 elem是[]Task
	TaskNum           int            //任务数量
}

func NewDispatcher(id int, ctx *Ctx) *Dispatcher {
	dis := &Dispatcher{
		ctx:               ctx.Context,
		Id:                id,
		Options:           ctx.Opts,
		TaskChunkCh:       make(chan TaskChunk, ctx.Opts.DisChSize),
		WaitingTasksQueue: list.New(),
	}

	go dis.run()

	return dis
}

func (d *Dispatcher) Submit(tasks []Task) error {
	submitTime := time.Now()
	taskLen := len(tasks)
	if taskLen == 0 {
		return nil
	}
	if taskLen > d.MaxBatchSize {
		d.ErrLogFn("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.MaxBatchSize)
		return fmt.Errorf("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.MaxBatchSize)
	}
	d.TaskChunkCh <- TaskChunk{
		SubmitTime: submitTime,
		Tasks:      tasks,
	}
	return nil
}

//优化这里的效率
func (d *Dispatcher) tryGetFetchTasks(mustGteMinBatchSize bool) (map[string][]Task, bool) {
	queueLen := d.WaitingTasksQueue.Len()
	if queueLen == 0 {
		return nil, false
	}
	//如果taskNum都没有大于最小批量，其实也没必要往下遍历了
	if mustGteMinBatchSize && d.TaskNum < d.MinBatchSize {
		return nil, false
	}
	gteMinBatchSize := false //是否大于等于最小批量大小
	needFetchElems := make([]*list.Element, 0, queueLen)
	visitedKeys := make(map[string]struct{})

	for el := d.WaitingTasksQueue.Front(); el != nil; el = el.Next() {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
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
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			if _, ok := itemKey2Tasks[t.ItemKey]; !ok {
				itemKey2Tasks[t.ItemKey] = []Task{t}
			} else {
				itemKey2Tasks[t.ItemKey] = append(itemKey2Tasks[t.ItemKey], t)
			}
		}
		d.TaskNum -= len(taskChunk.Tasks)
		d.WaitingTasksQueue.Remove(el)
	}

	return itemKey2Tasks, true
}

// 同一批不会拆分
func (d *Dispatcher) run() {
	ticker := time.NewTicker(d.CheckInterval)
	for {
		select {
		case taskChunk := <-d.TaskChunkCh:
			d.WaitingTasksQueue.PushBack(taskChunk)
			d.TaskNum += len(taskChunk.Tasks)
			//判断下
			key2Tasks, needFetch := d.tryGetFetchTasks(true)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				d.DebugLogFn("id=%v,fetch taskChunk by tasksCh\n", d.Id)
			}
		case <-ticker.C:
			key2Tasks, needFetch := d.tryGetFetchTasks(false)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				d.DebugLogFn("id=%v,fetch taskChunk by ticker\n", d.Id)
			}

		case <-d.ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) fetch(ctx context.Context, roomId2Tasks map[string][]Task) {
	roomIds := make([]string, 0, len(roomId2Tasks))
	for roomId := range roomId2Tasks {
		roomIds = append(roomIds, roomId)
	}
	roomId2Info, err := d.BatchGetFn(ctx, roomIds)
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
				if !d.IgnoreItemResNotExist {
					task.ErrCh <- fmt.Errorf("disId=%v,not found roomid:%s", d.Id, roomId)
					close(task.ErrCh)
				} else {
					task.ResCh <- nil
					close(task.ResCh)
				}

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
	err := disManager.Submit(taskList)
	if err != nil {
		return nil, err
	}

	key2Res := make(map[string]interface{})
	for _, task := range taskList {
		select {
		case res := <-task.ResCh:
			if res != nil {
				key2Res[task.ItemKey] = res
			}
		case err := <-task.ErrCh:
			return nil, err
		}
	}
	return key2Res, nil
}
