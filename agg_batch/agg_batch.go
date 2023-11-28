package aggbatch

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
)

var ErrDispatcherClosed = fmt.Errorf("dispatcher closed")

type MGetFn func(ctx context.Context, meteData map[string]interface{}, keys []interface{}) (map[interface{}]interface{}, error)
type LogFn func(format string, v ...interface{})

type Task struct {
	TaskId  string           //任务id
	ItemKey interface{}      //item key
	ResCh   chan interface{} //结果channel
	ErrCh   chan error       //错误channel
}

type DisCfg struct {
	DisChSize             int   `default:"500"`  //分发器channel大小
	CheckIntervalMs       int64 `default:"5"`    //检查间隔  time.Millisecond
	MinBatchSize          int   `default:"20"`   //最小批量大小
	MaxBatchSize          int   `default:"30"`   //最大批量大小
	IgnoreItemResNotExist bool  `default:"true"` //忽略item查询结果不存在
	FetchTimeoutMs        int64 `default:"1000"` //获取数据超时时间 time.Millisecond
}

// new default dispatcher config
func newDefaultDisCfg() DisCfg {
	return DisCfg{
		DisChSize:             500,
		CheckIntervalMs:       5,
		MinBatchSize:          20,
		MaxBatchSize:          30,
		FetchTimeoutMs:        1000,
		IgnoreItemResNotExist: true,
	}
}

type AggCfg struct {
	ShardingNum     int   `default:"1"`  //分片数
	StatIntervalSec int64 `default:"60"` //统计间隔
	DisCfg
}

// new default agg config
func NewDefaultAggCfg() AggCfg {
	return AggCfg{
		ShardingNum:     1,
		StatIntervalSec: 60,
		DisCfg:          newDefaultDisCfg(),
	}
}

type AggCtx struct {
	Ctx        context.Context
	AggCfg     AggCfg
	MetaData   map[string]interface{} //元数据 用于传递一些额外的信息
	MGetFn     MGetFn                 //批量查询函数
	ErrLogFn   LogFn
	DebugLogFn LogFn
}

func NewAggCtx(
	ctx context.Context,
	metaData map[string]interface{},
	cfg AggCfg, mgetFn MGetFn,
	ErrLogFn LogFn, DebugLogFn LogFn) *AggCtx {
	return &AggCtx{
		Ctx:        ctx,
		MetaData:   metaData,
		AggCfg:     cfg,
		MGetFn:     mgetFn,
		ErrLogFn:   ErrLogFn,
		DebugLogFn: DebugLogFn,
	}
}

type Agg struct {
	aggCtx *AggCtx

	atomicDs atomic.Value //[]*Dispatcher

	isClose      *atomic.Bool //是否关闭
	disCtxCancel context.CancelFunc
}

func NewAgg(aggCtx *AggCtx) *Agg {
	if aggCtx.AggCfg.ShardingNum <= 0 {
		panic("shardingNum must > 0")
	}
	if aggCtx.AggCfg.CheckIntervalMs <= 0 {
		panic("checkInterval must > 0")
	}
	if aggCtx.AggCfg.DisChSize <= 0 {
		panic("disChSize must > 0")
	}
	if aggCtx.AggCfg.MinBatchSize <= 0 {
		panic("minBatchSize must > 0")
	}
	if aggCtx.AggCfg.MaxBatchSize < aggCtx.AggCfg.MinBatchSize {
		panic("maxBatchSize must >= minBatchSize")
	}
	if aggCtx.MGetFn == nil {
		panic("batchGetFn must not be nil")
	}
	if aggCtx.ErrLogFn == nil {
		aggCtx.ErrLogFn = log.Printf
	}
	if aggCtx.DebugLogFn == nil {
		aggCtx.DebugLogFn = log.Printf
	}
	if aggCtx.AggCfg.StatIntervalSec <= 0 {
		aggCtx.AggCfg.StatIntervalSec = 60
	}

	ds := make([]*Dispatcher, aggCtx.AggCfg.ShardingNum, aggCtx.AggCfg.ShardingNum)
	disCtx, disCancel := context.WithCancel(aggCtx.Ctx)
	now := time.Now().Unix()
	for i := 0; i < aggCtx.AggCfg.ShardingNum; i++ {
		ds[i] = NewDispatcher(disCtx, GenDispatcherId(i, now), aggCtx)
	}
	atomicDs := atomic.Value{}
	atomicDs.Store(ds)

	isClose := atomic.Bool{}
	isClose.Store(false)
	manager := Agg{
		aggCtx:       aggCtx,
		atomicDs:     atomicDs,
		disCtxCancel: disCancel,
		isClose:      &isClose,
	}

	go manager.run()
	return &manager
}

func (this *Agg) stat() int {
	ds := this.atomicDs.Load().([]*Dispatcher)
	var (
		totalSubmitChunkNum   int64
		totalRecvChunkNum     int64
		totalFetchChunkNum    int64
		totalDelay            int64
		totalRecvDelay        int64
		totalMergeNum         int64
		totalReduceCallApiNum int64
		totalCallApiNum       int64
	)
	for _, d := range ds {
		totalRecvChunkNum += d.Stat.RecvTaskChunkNum.Load()
		totalSubmitChunkNum += d.Stat.SubmitTaskChunkNum.Load()
		totalFetchChunkNum += d.Stat.FetchTaskChunkNum.Load()
		totalDelay += d.Stat.TotalDelay.Load()
		totalMergeNum += d.Stat.MergeNum.Load()
		totalReduceCallApiNum += d.Stat.TotalReduceCallApiNum.Load()
		totalRecvDelay += d.Stat.TotalRecvDelay.Load()
		totalCallApiNum += d.Stat.TotalCallApiNum.Load()
		d.ClearStat()
	}
	totalDelayMs := float64(totalDelay) / float64(1000)
	avgDelayMs := float64(totalDelayMs) / float64(totalFetchChunkNum)
	totalRecvDelayMs := float64(totalRecvDelay) / float64(1000)
	avgRecvDelayMs := float64(totalRecvDelayMs) / float64(totalRecvChunkNum)
	this.aggCtx.ErrLogFn(
		"submitChunkNum=%v,recvTaskChunkNum=%v,fetchChunkNum=%v,mergeNum=%v,"+
			"avgDelayMs=%v,avgRecvDelayMs=%v,callApiNum=%v,reduceCallApiNum=%v,goNum=%v",
		totalSubmitChunkNum, totalRecvChunkNum, totalFetchChunkNum, totalMergeNum, avgDelayMs, avgRecvDelayMs, totalCallApiNum, totalReduceCallApiNum, runtime.NumGoroutine())
	return this.calShardingNum(avgDelayMs, len(ds))
}

func (this *Agg) calShardingNum(avgDelayMs float64, curShardingNum int) int {
	if avgDelayMs <= float64(this.aggCtx.AggCfg.CheckIntervalMs) {
		return curShardingNum
	}

	this.aggCtx.ErrLogFn("calShardingNum,avgDelayMs=%v,CheckIntervalMs=%v", avgDelayMs, this.aggCtx.AggCfg.CheckIntervalMs)
	return curShardingNum + 1
}

func (this *Agg) run() {
	ticker := time.NewTicker(time.Duration(this.aggCtx.AggCfg.StatIntervalSec) * time.Second)
	for {
		select {
		case <-this.aggCtx.Ctx.Done():
			this.isClose.Store(true)
			this.disCtxCancel()
			this.aggCtx.ErrLogFn("DispatcherManager done")
			return
		case <-ticker.C:
			now := time.Now().Unix()
			shardingNum := this.stat()
			curShardingNum := len(this.atomicDs.Load().([]*Dispatcher))
			if curShardingNum == shardingNum {
				continue
			}
			this.aggCtx.ErrLogFn("replace ds,newShardingNum=%v,now=%v", shardingNum, now)
			ds := make([]*Dispatcher, shardingNum, shardingNum)
			disCtx, disCancel := context.WithCancel(this.aggCtx.Ctx)
			for i := 0; i < shardingNum; i++ {
				ds[i] = NewDispatcher(disCtx, GenDispatcherId(i, now), this.aggCtx)
			}
			this.atomicDs.Store(ds)
			this.disCtxCancel()
			this.disCtxCancel = disCancel
		}
	}
}

func GenDispatcherId(idx int, now int64) string {
	//gen string
	return fmt.Sprintf("%d_%d", now, idx)
}

func (this *Agg) hash(size int) int {
	return rand.Intn(100) % size
}

func (this *Agg) Submit(ctx context.Context, tasks []Task) error {
	if this.isClose.Load() {
		return ErrDispatcherClosed
	}
	ds := this.atomicDs.Load().([]*Dispatcher)
	return ds[this.hash(len(ds))].Submit(ctx, tasks)
}

type TaskChunk struct {
	SubmitTime time.Time //提交时间
	Tasks      []Task
}

// 统计信息
type DispatcherStat struct {
	SubmitTaskChunkNum    atomic.Int64 //提交的task chunk 数量
	RecvTaskChunkNum      atomic.Int64 //接收的task chunk 数量
	FetchTaskChunkNum     atomic.Int64 //处理的task thunk 数量
	TotalDelay            atomic.Int64 //总延迟(从提交到执行)
	TotalRecvDelay        atomic.Int64 //总接收延迟(从提交channel到接收)
	MergeNum              atomic.Int64 //合并key的数量
	TotalCallApiNum       atomic.Int64 //调用api的数量
	TotalReduceCallApiNum atomic.Int64 //减少的调用api的数量

}
type Dispatcher struct {
	ctx    context.Context
	aggCtx *AggCtx

	Stat DispatcherStat

	Id                  string         //id
	TaskChunkCh         chan TaskChunk //任务channel
	WaitingTasksQueue   *list.List     //等待中的任务队列 elem是[]Task
	TaskNum             int            //任务数量
	LastFetchMs         int64          //最近一次fetch的时间戳 ms
	LastRecvTaskChunkTs int64          //最近一次接收task chunk的时间戳 s
	IsClose             bool
}

func NewDispatcher(ctx context.Context, id string, aggCtx *AggCtx) *Dispatcher {
	dis := &Dispatcher{
		ctx:               ctx,
		Id:                id,
		aggCtx:            aggCtx,
		TaskChunkCh:       make(chan TaskChunk, aggCtx.AggCfg.DisChSize),
		WaitingTasksQueue: list.New(),
	}

	go dis.run()

	return dis
}

// clear stat
func (d *Dispatcher) ClearStat() {
	d.Stat.RecvTaskChunkNum.Store(0)
	d.Stat.SubmitTaskChunkNum.Store(0)
	d.Stat.FetchTaskChunkNum.Store(0)
	d.Stat.TotalDelay.Store(0)
	d.Stat.TotalRecvDelay.Store(0)
	d.Stat.MergeNum.Store(0)
	d.Stat.TotalReduceCallApiNum.Store(0)
	d.Stat.TotalCallApiNum.Store(0)
}

func (d *Dispatcher) Submit(ctx context.Context, tasks []Task) error {
	submitTime := time.Now()
	taskLen := len(tasks)
	if taskLen == 0 {
		return nil
	}
	if taskLen > d.aggCtx.AggCfg.MaxBatchSize {
		d.aggCtx.ErrLogFn("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.aggCtx.AggCfg.MaxBatchSize)
		return fmt.Errorf("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.aggCtx.AggCfg.MaxBatchSize)
	}
	chunk := TaskChunk{
		SubmitTime: submitTime,
		Tasks:      tasks,
	}
	d.Stat.SubmitTaskChunkNum.Add(1)
	select {
	case <-ctx.Done():
		return fmt.Errorf("submit err=%v", ctx.Err())
	case d.TaskChunkCh <- chunk:
		return nil
	}
}

// 优化这里的效率
func (d *Dispatcher) tryGetFetchTasks(mustGteMinBatchSize bool) (map[interface{}][]Task, bool) {
	queueLen := d.WaitingTasksQueue.Len()
	if queueLen == 0 {
		return nil, false
	}
	//如果taskNum都没有大于最小批量，其实也没必要往下遍历了
	if mustGteMinBatchSize && d.TaskNum < d.aggCtx.AggCfg.MinBatchSize {
		return nil, false
	}
	gteMinBatchSize := false //是否大于等于最小批量大小
	needFetchElems := make([]*list.Element, 0, queueLen)

	visitedKeys := make(map[interface{}]struct{})
	for el := d.WaitingTasksQueue.Front(); el != nil; el = el.Next() {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			visitedKeys[t.ItemKey] = struct{}{}
		}
		curNum := len(visitedKeys)
		if curNum >= d.aggCtx.AggCfg.MinBatchSize {
			gteMinBatchSize = true
			if curNum > d.aggCtx.AggCfg.MaxBatchSize {
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

	itemKey2Tasks := make(map[interface{}][]Task)

	now := time.Now()
	for _, el := range needFetchElems {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			if _, ok := itemKey2Tasks[t.ItemKey]; !ok {
				itemKey2Tasks[t.ItemKey] = []Task{t}
			} else {
				d.Stat.MergeNum.Add(1)
				itemKey2Tasks[t.ItemKey] = append(itemKey2Tasks[t.ItemKey], t)
			}
		}
		d.TaskNum -= len(taskChunk.Tasks)
		d.WaitingTasksQueue.Remove(el)
		d.Stat.FetchTaskChunkNum.Add(1)
		d.Stat.TotalDelay.Add(now.Sub(taskChunk.SubmitTime).Microseconds())
	}
	d.Stat.TotalReduceCallApiNum.Add(int64(len(needFetchElems) - 1))
	d.Stat.TotalCallApiNum.Add(1)
	d.LastFetchMs = now.UnixMilli()
	return itemKey2Tasks, true
}

// 同一批不会拆分
func (d *Dispatcher) run() {
	ticker := time.NewTicker(time.Duration(d.aggCtx.AggCfg.CheckIntervalMs) * time.Millisecond)
	exitTicker := time.NewTicker(10 * time.Second)
	for {
		select {
		case taskChunk := <-d.TaskChunkCh:
			now := time.Now()
			d.LastRecvTaskChunkTs = now.Unix()
			d.Stat.RecvTaskChunkNum.Add(1)
			d.Stat.TotalRecvDelay.Add(now.Sub(taskChunk.SubmitTime).Microseconds())
			d.WaitingTasksQueue.PushBack(taskChunk)
			d.TaskNum += len(taskChunk.Tasks)
			//判断下
			key2Tasks, needFetch := d.tryGetFetchTasks(true)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				//重置ticker,优化点,聚合效果更好
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(d.aggCtx.AggCfg.CheckIntervalMs) * time.Millisecond)
				d.aggCtx.DebugLogFn("id=%v,fetch taskChunk by tasksCh", d.Id)
			}
		case <-ticker.C:
			key2Tasks, needFetch := d.tryGetFetchTasks(false)
			if needFetch {
				go d.fetch(context.Background(), key2Tasks)
				d.aggCtx.DebugLogFn("id=%v,fetch taskChunk by ticker", d.Id)
			}
		case <-d.ctx.Done():
			d.aggCtx.DebugLogFn("id=%v,ctx done", d.Id)
			d.IsClose = true
		case <-exitTicker.C:
			//如何保证所有的任务都被处理完了(close+10s内没有任务进来)
			if d.IsClose && d.LastRecvTaskChunkTs+10 < time.Now().Unix() {
				d.aggCtx.ErrLogFn("id=%v,exitTicker done", d.Id)
				return
			}
		}
	}
}

func (d *Dispatcher) fetch(ctx context.Context, key2Tasks map[interface{}][]Task) {
	keys := make([]interface{}, 0, len(key2Tasks))
	for k := range key2Tasks {
		keys = append(keys, k)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(d.aggCtx.AggCfg.FetchTimeoutMs)*time.Millisecond)
	defer cancel()
	key2Info, err := d.aggCtx.MGetFn(timeoutCtx, d.aggCtx.MetaData, keys)
	if err != nil {
		for _, tasks := range key2Tasks {
			for _, task := range tasks {
				task.ErrCh <- fmt.Errorf("disId=%v fetch err=%w", d.Id, err)
				close(task.ErrCh)
			}
		}
		return
	}
	for key, tasks := range key2Tasks {
		info, ok := key2Info[key]
		if !ok {
			for _, task := range tasks {
				if !d.aggCtx.AggCfg.IgnoreItemResNotExist {
					task.ErrCh <- fmt.Errorf("disId=%v,not found key:%s", d.Id, key)
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

func GenTaskId() string {
	//时间戳+随机数+随机数
	return fmt.Sprintf("%d_%d_%d", time.Now().UnixNano(), rand.Intn(1000), rand.Intn(1000))
}

// 底层被dispatch调度，聚合发送请求
// 请求量很小的情况下，可能会增加设定的CheckInterval延迟
func MGetWarp(ctx context.Context, disManager *Agg, keys []interface{}) (map[interface{}]interface{}, error) {
	taskList := make([]Task, 0, len(keys))
	for _, key := range keys {
		task := Task{
			TaskId:  GenTaskId(),
			ItemKey: key,
			ResCh:   make(chan interface{}, 1),
			ErrCh:   make(chan error, 1),
		}
		taskList = append(taskList, task)
	}
	err := disManager.Submit(ctx, taskList)
	if err != nil {
		return nil, err
	}

	key2Res := make(map[interface{}]interface{})
	for _, task := range taskList {
		select {
		case res := <-task.ResCh:
			if res != nil {
				key2Res[task.ItemKey] = res
			}
		case err := <-task.ErrCh:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return key2Res, nil
}
