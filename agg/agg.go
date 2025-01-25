package agg

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
)

var AggClosedError = fmt.Errorf("agg closed")

type IMGet interface {
	MGet(ctx context.Context, keys []interface{}) (map[interface{}]interface{}, error)
}
type MGetFn func(ctx context.Context, keys []interface{}) (map[interface{}]interface{}, error)

type LogFn func(format string, v ...interface{})

type Task struct {
	TaskId string           //任务id
	Key    interface{}      // key
	ResCh  chan interface{} //结果channel
	ErrCh  chan error       //错误channel
}

type AggConfig struct {
	ChanSize             int   `default:"500"`  //分发器channel大小
	MaxWaitMs            int64 `default:"3"`    //最大等待时长time.Millisecond
	MinBatchSize         int   `default:"20"`   //最小批量大小
	MaxBatchSize         int   `default:"30"`   //最大批量大小
	IgnoreKeyResNotExist bool  `default:"true"` //忽略item查询结果不存在
	MGetTimeoutMs        int64 `default:"300"`  //获取数据超时时间 time.Millisecond
}

func (this *AggConfig) Validate() error {
	if this.ChanSize <= 0 {
		return fmt.Errorf("ChanSize must > 0")
	}
	if this.MaxWaitMs <= 0 {
		return fmt.Errorf("CheckIntervalMs must > 0")
	}
	if this.MinBatchSize <= 0 {
		return fmt.Errorf("MinBatchSize must > 0")
	}
	if this.MaxBatchSize <= 0 {
		return fmt.Errorf("MaxBatchSize must > 0")
	}
	if this.MaxBatchSize < this.MinBatchSize {
		return fmt.Errorf("MaxBatchSize must >= MinBatchSize")
	}
	if this.MGetTimeoutMs <= 0 {
		return fmt.Errorf("FetchTimeoutMs must > 0")
	}
	return nil
}

// new default config
func NewDefaultAggConfig() AggConfig {
	return AggConfig{
		ChanSize:             500,
		MaxWaitMs:            2,
		MinBatchSize:         20,
		MaxBatchSize:         30,
		MGetTimeoutMs:        300,
		IgnoreKeyResNotExist: true,
	}
}

type TaskChunk struct {
	SubmitTime time.Time //提交时间
	Tasks      []Task
}

type SyncAggStat struct {
	RecvTaskChunkNum      int64 //接收的task chunk 数量
	ProcessTaskChunkNum   int64 //处理的task thunk 数量
	TotalDelay            int64 //总延迟(从提交到执行)
	TotalRecvDelay        int64 //总接收延迟(从提交channel到接收)
	MergeNum              int64 //合并key的数量
	TotalCallApiNum       int64 //调用api的数量
	TotalReduceCallApiNum int64 //减少的调用api的数量
	TimeoutTaskChunkNum   int64
	TimeoutTickerNum      int64 //ticker触发次数
}

// 统计信息
type AggStat struct {
	SubmitTaskChunkNum int64 //提交的task chunk 数量
	SyncAggStat
}
type DisplayAggStat struct {
	GoNum               int64
	SubmitTaskChunkNum  int64   //提交的task chunk 数量
	RecvTaskChunkNum    int64   //接收的task chunk 数量
	ProcessTaskChunkNum int64   //处理的task thunk 数量
	AvgDelayMs          float64 //平均总延迟
	AvgRecvDelayMs      float64 //接收任务的延迟
	MergeNum            int64   //合并key的数量
	CallApiNum          int64   //调用api的数量
	ReduceCallApiNum    int64   //减少的调用api的数量
	TimeoutTaskChunkNum int64   //等待超时的task chunk 数量
	TimeoutTickerNum    int64   //超时ticker的数量
}
type Agg struct {
	Id       string //id
	ctx      context.Context
	config   AggConfig
	errorLog LogFn
	debugLog LogFn

	//mgetFn mgetImpl 二选一
	mgetFn   MGetFn //批量查询函数
	mgetImpl IMGet  //批量查询

	taskChunkCh         chan TaskChunk //任务channel
	clearStatCh         chan struct{}
	taskChunkQueue      *list.List //等待中的任务队列 elem是[]Task
	taskNum             int        //任务数量
	lastRecvTaskChunkTs int64      //最近一次接收task chunk的时间戳
	isClosed            bool

	stat AggStat
}

type BuildAggParams struct {
	Config AggConfig
	//mgetFn mgetImpl 二选一
	MGetFn   MGetFn //批量查询函数
	MGetImpl IMGet  //批量查询

	//可以不设置
	Id       string
	ErrorLog LogFn
	DebugLog LogFn
}

func (this *BuildAggParams) Validate() error {
	if this.MGetFn == nil && this.MGetImpl == nil {
		return errors.New("mgetFn or mgetImpl must be set")
	}
	if this.MGetFn != nil && this.MGetImpl != nil {
		return errors.New("mgetFn and mgetImpl can not be set at the same time")
	}
	err := this.Config.Validate()
	if err != nil {
		return fmt.Errorf("config validate error:%w", err)
	}
	return nil
}

func NewAgg(ctx context.Context, params BuildAggParams) (*Agg, error) {
	err := params.Validate()
	if err != nil {
		return nil, fmt.Errorf("params validate error:%w", err)
	}
	agg := &Agg{
		ctx:            ctx,
		Id:             params.Id,
		config:         params.Config,
		errorLog:       params.ErrorLog,
		debugLog:       params.DebugLog,
		mgetFn:         params.MGetFn,
		mgetImpl:       params.MGetImpl,
		taskChunkCh:    make(chan TaskChunk, params.Config.ChanSize),
		taskChunkQueue: list.New(),
		clearStatCh:    make(chan struct{}, 1),
	}

	go agg.run()

	return agg, nil
}

func (this *Agg) SubmitAndWait(ctx context.Context, keys []interface{}) (map[interface{}]interface{}, error) {
	taskList := make([]Task, 0, len(keys))
	for _, key := range keys {
		task := Task{
			TaskId: genTaskId(),
			Key:    key,
			ResCh:  make(chan interface{}, 1),
			ErrCh:  make(chan error, 1),
		}
		taskList = append(taskList, task)
	}
	err := this.Submit(ctx, taskList)
	if err != nil {
		return nil, err
	}

	key2Res := make(map[interface{}]interface{})
	for _, task := range taskList {
		select {
		case res := <-task.ResCh:
			if res != nil {
				key2Res[task.Key] = res
			}
		case err := <-task.ErrCh:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return key2Res, nil
}

func (d *Agg) Stat() DisplayAggStat {
	s := d.stat
	var avgDelayMs float64 = 0
	if s.ProcessTaskChunkNum != 0 {
		avgDelayMs = (float64(s.TotalDelay) / float64(1000)) / float64(s.ProcessTaskChunkNum)
	}
	var avgRecvDelayMs float64 = 0
	if s.RecvTaskChunkNum != 0 {
		avgRecvDelayMs = (float64(s.TotalRecvDelay) / float64(1000)) / float64(s.RecvTaskChunkNum)
	}

	ds := DisplayAggStat{
		GoNum:               int64(runtime.NumGoroutine()),
		SubmitTaskChunkNum:  atomic.LoadInt64(&s.SubmitTaskChunkNum),
		RecvTaskChunkNum:    s.RecvTaskChunkNum,
		ProcessTaskChunkNum: s.ProcessTaskChunkNum,
		MergeNum:            s.MergeNum,
		CallApiNum:          s.TotalCallApiNum,
		ReduceCallApiNum:    s.TotalReduceCallApiNum,
		AvgDelayMs:          avgDelayMs,
		AvgRecvDelayMs:      avgRecvDelayMs,
		TimeoutTaskChunkNum: s.TimeoutTaskChunkNum,
		TimeoutTickerNum:    s.TimeoutTickerNum,
	}
	return ds
}

func (d *Agg) tryErrorLog(format string, v ...interface{}) {
	if d.errorLog != nil {
		d.errorLog(format, v...)
	}
}
func (d *Agg) tryDebugLog(format string, v ...interface{}) {
	if d.debugLog != nil {
		d.debugLog(format, v...)
	}
}

func (d *Agg) Submit(ctx context.Context, tasks []Task) error {
	submitTime := time.Now()
	taskLen := len(tasks)
	if taskLen == 0 {
		return nil
	}
	if taskLen > d.config.MaxBatchSize {
		d.tryErrorLog("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.config.MaxBatchSize)
		return fmt.Errorf("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.config.MaxBatchSize)
	}
	chunk := TaskChunk{
		SubmitTime: submitTime,
		Tasks:      tasks,
	}
	atomic.AddInt64(&d.stat.SubmitTaskChunkNum, 1)
	select {
	case <-ctx.Done():
		return fmt.Errorf("submit err=%w", ctx.Err())
	case d.taskChunkCh <- chunk:
		return nil
	}
}

func (d *Agg) getTimeoutTasks() (map[interface{}][]Task, bool) {
	queueLen := d.taskChunkQueue.Len()
	if queueLen == 0 {
		return nil, false
	}
	key2Tasks := make(map[interface{}][]Task)
	now := time.Now()
	elemNum := 0
	for el := d.taskChunkQueue.Front(); el != nil; el = el.Next() {
		taskChunk := el.Value.(TaskChunk)
		//是否timeout
		if now.Sub(taskChunk.SubmitTime) < time.Duration(d.config.MaxWaitMs)*time.Millisecond {
			break
		}
		for _, t := range taskChunk.Tasks {
			if _, ok := key2Tasks[t.Key]; !ok {
				key2Tasks[t.Key] = []Task{t}
			} else {
				d.stat.MergeNum += 1
				key2Tasks[t.Key] = append(key2Tasks[t.Key], t)
			}
		}
		d.taskNum -= len(taskChunk.Tasks)
		d.stat.ProcessTaskChunkNum += 1
		d.stat.TimeoutTaskChunkNum += 1
		d.stat.TotalDelay += now.Sub(taskChunk.SubmitTime).Microseconds()
		elemNum += 1
		d.taskChunkQueue.Remove(el)
	}
	if len(key2Tasks) == 0 {
		return nil, false
	}

	d.stat.TotalReduceCallApiNum += int64(elemNum) - 1
	d.stat.TotalCallApiNum += 1
	return key2Tasks, true
}

func (d *Agg) tryGetTasks() (map[interface{}][]Task, bool) {
	//如果taskNum都没有大于最小批量，其实也没必要往下遍历了
	if d.taskNum < d.config.MinBatchSize {
		return nil, false
	}
	greaterMinSize := false //是否大于等于最小批量大小
	elems := make([]*list.Element, 0, d.taskChunkQueue.Len())

	visitedKeys := make(map[interface{}]struct{})
	for el := d.taskChunkQueue.Front(); el != nil; el = el.Next() {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			visitedKeys[t.Key] = struct{}{}
		}
		curNum := len(visitedKeys)
		if curNum >= d.config.MinBatchSize {
			greaterMinSize = true
			if curNum > d.config.MaxBatchSize {
				//那此次遍历elem不能添加进去
				break
			} else {
				elems = append(elems, el)
				break
			}
		} else {
			elems = append(elems, el)
		}
	}
	if !greaterMinSize {
		return nil, false
	}

	key2Tasks := make(map[interface{}][]Task)

	now := time.Now()
	for _, el := range elems {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			if _, ok := key2Tasks[t.Key]; !ok {
				key2Tasks[t.Key] = []Task{t}
			} else {
				d.stat.MergeNum += 1
				key2Tasks[t.Key] = append(key2Tasks[t.Key], t)
			}
		}
		d.taskNum -= len(taskChunk.Tasks)
		d.stat.ProcessTaskChunkNum += 1
		d.stat.TotalDelay += now.Sub(taskChunk.SubmitTime).Microseconds()
		d.taskChunkQueue.Remove(el)
	}

	d.stat.TotalReduceCallApiNum += int64(len(elems) - 1)
	d.stat.TotalCallApiNum += 1
	return key2Tasks, true
}

// 同一批不会拆分
func (d *Agg) run() {
	timeoutTicker := time.NewTicker(time.Millisecond)
	exitTicker := time.NewTicker(10 * time.Second)
	for {
		select {
		case taskChunk := <-d.taskChunkCh:
			now := time.Now()
			d.lastRecvTaskChunkTs = now.Unix()
			d.stat.RecvTaskChunkNum += 1
			d.stat.TotalRecvDelay += now.Sub(taskChunk.SubmitTime).Microseconds()
			d.taskChunkQueue.PushBack(taskChunk)
			d.taskNum += len(taskChunk.Tasks)
			//判断下
			key2Tasks, get := d.tryGetTasks()
			if get {
				go d.process(context.Background(), key2Tasks)
				d.tryDebugLog("id=%v,fetch taskChunk by tasksCh", d.Id)
			}
		case <-timeoutTicker.C:
			d.stat.TimeoutTickerNum += 1
			key2Tasks, exist := d.getTimeoutTasks()
			if exist {
				go d.process(context.Background(), key2Tasks)
				d.tryDebugLog("id=%v,fetch taskChunk by ticker", d.Id)
			}
		case <-d.ctx.Done():
			if !d.isClosed {
				d.tryErrorLog("id=%v,ctx done", d.Id)
				d.isClosed = true
			}
		case <-exitTicker.C:
			//如何保证所有的任务都被处理完了(close+10s内没有任务进来)
			if d.isClosed && d.lastRecvTaskChunkTs+10 < time.Now().Unix() {
				d.tryErrorLog("id=%v,exitTicker done", d.Id)
				return
			}
		case <-d.clearStatCh:
			atomic.StoreInt64(&d.stat.SubmitTaskChunkNum, 0)
			d.stat.SyncAggStat = SyncAggStat{}
		}
	}
}

func (d *Agg) process(ctx context.Context, key2Tasks map[interface{}][]Task) {
	keys := make([]interface{}, 0, len(key2Tasks))
	for k := range key2Tasks {
		keys = append(keys, k)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(d.config.MGetTimeoutMs)*time.Millisecond)
	defer cancel()

	var err error
	var key2Info map[interface{}]interface{}
	if d.mgetFn != nil {
		key2Info, err = d.mgetFn(timeoutCtx, keys)
	} else {
		key2Info, err = d.mgetImpl.MGet(timeoutCtx, keys)
	}
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
				if !d.config.IgnoreKeyResNotExist {
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

func genTaskId() string {
	//时间戳+随机数+随机数
	return fmt.Sprintf("%d_%d_%d", time.Now().UnixNano(), rand.Intn(1000), rand.Intn(1000))
}
