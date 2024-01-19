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

var AggMngClosedError = fmt.Errorf("agg mng closed")

type MGetFn func(ctx context.Context, meteData map[string]interface{}, keys []interface{}) (map[interface{}]interface{}, error)
type LogFn func(format string, v ...interface{})

type Task struct {
	TaskId string           //任务id
	Key    interface{}      //item key(eg:uid、roomId)
	ResCh  chan interface{} //结果channel
	ErrCh  chan error       //错误channel
}
type TaskChunk struct {
	SubmitTime time.Time //提交时间
	Tasks      []Task
}

type AggCfg struct {
	ChanSize              int   `default:"500"`  //dispather channel大小
	CheckIntervalMs       int64 `default:"5"`    //检查间隔  time.Millisecond
	MinBatchSize          int   `default:"20"`   //最小批量大小
	MaxBatchSize          int   `default:"30"`   //最大批量大小
	IgnoreItemResNotExist bool  `default:"true"` //忽略item查询结果不存在的错误
	MGetTimeoutMs         int64 `default:"1000"` //获取数据超时时间 time.Millisecond
	ErrLog                LogFn
	DebugLog              LogFn
	MGetFn                MGetFn
}

// new default agg config
func NewAggCfg() AggCfg {
	return AggCfg{
		ChanSize:              500,
		CheckIntervalMs:       5,
		MinBatchSize:          20,
		MaxBatchSize:          30,
		MGetTimeoutMs:         1000,
		IgnoreItemResNotExist: true,
		ErrLog:                log.Printf,
		DebugLog:              log.Printf,
	}
}

func (this *AggCfg) IsValid() error {
	if this.ChanSize <= 0 {
		return fmt.Errorf("ChSize must > 0")
	}
	if this.CheckIntervalMs <= 0 {
		return fmt.Errorf("CheckIntervalMs must > 0")
	}
	if this.MinBatchSize <= 0 {
		return fmt.Errorf("MinBatchSize must > 0")
	}
	if this.MaxBatchSize <= 0 {
		return fmt.Errorf("MaxBatchSize must > 0")
	}
	if this.MinBatchSize > this.MaxBatchSize {
		return fmt.Errorf("MinBatchSize must <= MaxBatchSize")
	}
	if this.MGetTimeoutMs <= 0 {
		return fmt.Errorf("FetchTimeoutMs must > 0")
	}

	if this.ErrLog == nil {
		return fmt.Errorf("ErrLog must not be nil")
	}
	if this.DebugLog == nil {
		return fmt.Errorf("DebugLog must not be nil")
	}
	if this.MGetFn == nil {
		return fmt.Errorf("MGetFn must not be nil")
	}
	return nil
}

type AggManager struct {
	ctx             context.Context
	name            string
	statIntervalSec int //统计间隔
	aggCfg          AggCfg

	debugLog LogFn
	errLog   LogFn

	atomicAggs   atomic.Value //[]*Agg
	aggNum       int          //聚合器数量
	isClose      int32        //是否关闭 1关闭
	aggCtxCancel context.CancelFunc

	aggStatChan chan AggStat
	aggStatSum  AggStat
}

type AggMngCfg struct {
	Name            string
	AggNum          int //聚合器数量
	StatIntervalSec int //统计间隔
	DebugLog        LogFn
	ErrLog          LogFn
}

func (this *AggMngCfg) IsValid() error {
	if this.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if this.AggNum <= 0 {
		return fmt.Errorf("aggNum must > 0")
	}
	if this.StatIntervalSec <= 0 {
		return fmt.Errorf("statIntervalSec must > 0")
	}
	if this.DebugLog == nil {
		return fmt.Errorf("debugLog must not be nil")
	}
	if this.ErrLog == nil {
		return fmt.Errorf("errLog must not be nil")
	}
	return nil
}

// new default
func NewAggMngCfg(name string) AggMngCfg {
	return AggMngCfg{
		Name:            name,
		AggNum:          1,
		StatIntervalSec: 60,
		DebugLog:        log.Printf,
		ErrLog:          log.Printf,
	}
}

func NewAggManager(ctx context.Context, mngCfg AggMngCfg, aggCfg AggCfg) *AggManager {
	if err := mngCfg.IsValid(); err != nil {
		panic(err)
	}
	if err := aggCfg.IsValid(); err != nil {
		panic(err)
	}
	aggMng := AggManager{
		ctx:             ctx,
		name:            mngCfg.Name,
		statIntervalSec: mngCfg.StatIntervalSec,
		aggCfg:          aggCfg,
		debugLog:        mngCfg.DebugLog,
		errLog:          mngCfg.ErrLog,
		isClose:         0,
		aggNum:          mngCfg.AggNum,
		aggStatChan:     make(chan AggStat, 100),
	}

	aggs := make([]*Agg, 0)
	aggCtx, aggCancel := context.WithCancel(ctx)
	now := time.Now().Unix()
	for i := 0; i < mngCfg.AggNum; i++ {
		aggId := genAggId(i, now, mngCfg.Name)
		agg := NewAgg(aggCtx, aggId, aggCfg, &aggMng)
		aggs = append(aggs, agg)
	}
	atomicAggs := atomic.Value{}
	atomicAggs.Store(aggs)
	aggMng.atomicAggs = atomicAggs
	aggMng.aggCtxCancel = aggCancel

	go aggMng.run()
	return &aggMng
}

func (this *AggManager) stat() {
	var (
		totalSubmitChunkNum   int64 = this.aggStatSum.SubmitTaskChunkNum
		totalRecvChunkNum     int64 = this.aggStatSum.RecvTaskChunkNum
		totalMGetChunkNum     int64 = this.aggStatSum.MGetTaskChunkNum
		totalDelay            int64 = this.aggStatSum.TotalDelay
		totalRecvDelay        int64 = this.aggStatSum.TotalRecvDelay
		totalMergeNum         int64 = this.aggStatSum.MergeNum
		totalReduceCallApiNum int64 = this.aggStatSum.TotalReduceCallApiNum
		totalCallApiNum       int64 = this.aggStatSum.TotalCallApiNum
	)
	this.aggStatSum = AggStat{}

	totalDelayMs := float64(totalDelay) / float64(1000)
	var avgDelayMs float64 = 0
	var avgRecvDelayMs float64 = 0
	if totalMGetChunkNum == 0 {
		avgDelayMs = 0
	} else {
		avgDelayMs = float64(totalDelayMs) / float64(totalMGetChunkNum)
	}
	if totalRecvChunkNum == 0 {
		avgRecvDelayMs = 0
	} else {
		avgRecvDelayMs = float64(totalRecvDelay) / float64(1000*totalRecvChunkNum)
	}
	this.errLog(
		"stat info:agg=%v,disNum=%v,goNum=%v,submitChunkNum=%v,recvTaskChunkNum=%v,fetchChunkNum=%v,"+
			"avgRecvDelayMs=%v,【mergeKeyNum=%v】【avgDelayMs=%v】【callApiNum=%v】【reduceCallApiNum=%v】",
		this.name, this.aggNum, runtime.NumGoroutine(),
		totalSubmitChunkNum, totalRecvChunkNum,
		totalMGetChunkNum,
		avgRecvDelayMs, totalMergeNum, avgDelayMs, totalCallApiNum, totalReduceCallApiNum)

	newAggNum := this.calAggNum(avgDelayMs, this.aggNum)
	if this.aggNum == newAggNum {
		return
	}
	now := time.Now().Unix()
	//进行扩容
	this.errLog("agg expansion,aggName=%v,oldAggNum=%v,newAggNum=%v,now=%v", this.name, this.aggNum, newAggNum, now)
	ds := make([]*Agg, newAggNum, newAggNum)
	aggCtx, aggCancel := context.WithCancel(this.ctx)
	for i := 0; i < newAggNum; i++ {
		ds[i] = NewAgg(aggCtx, genAggId(i, now, this.name), this.aggCfg, this)
	}
	this.atomicAggs.Store(ds)
	this.aggCtxCancel()
	this.aggCtxCancel = aggCancel
	this.aggNum = newAggNum
}

func (this *AggManager) calAggNum(avgDelayMs float64, curAggNum int) int {
	if avgDelayMs <= float64(this.aggCfg.CheckIntervalMs+2) {
		return curAggNum
	}
	return curAggNum + 1
}

func (this *AggManager) run() {
	ticker := time.NewTicker(time.Duration(this.statIntervalSec) * time.Second)
	for {
		select {
		case <-this.ctx.Done():
			atomic.StoreInt32(&this.isClose, 1)
			this.aggCtxCancel()
			this.errLog("AggManager done")
			return
		case stat := <-this.aggStatChan:
			this.sumStat(stat)
		case <-ticker.C:
			//定时统计+扩容
			this.stat()
		}
	}
}

func (this *AggManager) sumStat(stat AggStat) {
	this.aggStatSum.RecvTaskChunkNum += stat.RecvTaskChunkNum
	this.aggStatSum.SubmitTaskChunkNum += stat.SubmitTaskChunkNum
	this.aggStatSum.MGetTaskChunkNum += stat.MGetTaskChunkNum
	this.aggStatSum.TotalDelay += stat.TotalDelay
	this.aggStatSum.TotalRecvDelay += stat.TotalRecvDelay
	this.aggStatSum.MergeNum += stat.MergeNum
	this.aggStatSum.TotalCallApiNum += stat.TotalCallApiNum
	this.aggStatSum.TotalReduceCallApiNum += stat.TotalReduceCallApiNum
}

func genAggId(idx int, now int64, aggName string) string {
	//gen string
	return fmt.Sprintf("agg_%v_%d_%d", aggName, now, idx)
}

func (this *AggManager) hash(size int) int {
	return rand.Intn(100) % size
}

func (this *AggManager) Submit(ctx context.Context, tasks []Task) error {
	if atomic.LoadInt32(&this.isClose) == 1 {
		return AggMngClosedError
	}
	ds := this.atomicAggs.Load().([]*Agg)
	return ds[this.hash(len(ds))].Submit(ctx, tasks)
}

// 统计信息
type AggStat struct {
	SubmitTaskChunkNum    int64 //提交的task chunk 数量
	RecvTaskChunkNum      int64 //接收的task chunk 数量
	MGetTaskChunkNum      int64 //处理的task thunk 数量
	TotalDelay            int64 //总延迟(从提交到执行)
	TotalRecvDelay        int64 //总接收延迟(从提交channel到接收)
	MergeNum              int64 //合并key的数量
	TotalCallApiNum       int64 //调用api的数量
	TotalReduceCallApiNum int64 //减少的调用api的数量
}

type Agg struct {
	ctx    context.Context
	id     string //id
	aggCfg AggCfg

	aggMng *AggManager //聚合器

	stat                AggStat        //统计信息
	taskChunkCh         chan TaskChunk //任务channel,用于接收任务
	waitingTasksQueue   *list.List     //等待中的任务队列 elem是TaskChunk
	taskNum             int            //等待中的任务数量
	lastRecvTaskChunkTs int64          //最后一次接收task chunk的时间戳
	isClose             bool
}

func NewAgg(ctx context.Context, id string, aggCfg AggCfg, aggMng *AggManager) *Agg {
	agg := &Agg{
		ctx:               ctx,
		id:                id,
		aggMng:            aggMng,
		aggCfg:            aggCfg,
		taskChunkCh:       make(chan TaskChunk, aggCfg.ChanSize),
		waitingTasksQueue: list.New(),
	}

	go agg.run()

	return agg
}

// clear stat
func (d *Agg) clearStat() {
	d.stat = AggStat{}
}

func (d *Agg) Submit(ctx context.Context, tasks []Task) error {
	submitTime := time.Now()
	taskLen := len(tasks)
	if taskLen == 0 {
		return nil
	}
	if taskLen > d.aggCfg.MaxBatchSize {
		d.aggCfg.ErrLog("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.aggCfg.MaxBatchSize)
		return fmt.Errorf("taskLen > maxBatchSize, taskLen=%d, maxBatchSize=%d", taskLen, d.aggCfg.MaxBatchSize)
	}
	chunk := TaskChunk{
		SubmitTime: submitTime,
		Tasks:      tasks,
	}
	d.stat.SubmitTaskChunkNum++
	select {
	case <-ctx.Done():
		return fmt.Errorf("submit err=%v", ctx.Err())
	case d.taskChunkCh <- chunk:
		return nil
	}
}

// @mustGteMinSize 是否必须要大于等于最小批量
func (d *Agg) tryGetTasks(mustGteMinSize bool) (map[interface{}][]Task, bool) {
	queueLen := d.waitingTasksQueue.Len()
	if queueLen == 0 {
		return nil, false
	}
	//如果taskNum都没有大于最小批量，其实也没必要往下遍历了
	if mustGteMinSize && d.taskNum < d.aggCfg.MinBatchSize {
		return nil, false
	}
	gteMinBatchSize := false //是否大于等于最小批量大小
	elems := make([]*list.Element, 0, queueLen)

	visitedKeys := make(map[interface{}]struct{}) //用来去重统计key的数量
	for el := d.waitingTasksQueue.Front(); el != nil; el = el.Next() {
		taskChunk := el.Value.(TaskChunk)
		for _, t := range taskChunk.Tasks {
			visitedKeys[t.Key] = struct{}{}
		}
		keyNum := len(visitedKeys)
		if keyNum >= d.aggCfg.MinBatchSize {
			gteMinBatchSize = true
			if keyNum > d.aggCfg.MaxBatchSize {
				//那此次遍历taskChunk不能添加进去
				break
			} else {
				elems = append(elems, el)
				break
			}
		} else {
			elems = append(elems, el)
		}
	}
	if !gteMinBatchSize && mustGteMinSize {
		return nil, false
	}
	if len(elems) == 0 {
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
				d.stat.MergeNum++
				key2Tasks[t.Key] = append(key2Tasks[t.Key], t) //合并key
			}
		}
		d.taskNum -= len(taskChunk.Tasks)
		d.waitingTasksQueue.Remove(el)
		d.stat.MGetTaskChunkNum++
		d.stat.TotalDelay += now.Sub(taskChunk.SubmitTime).Microseconds()
	}
	d.stat.TotalReduceCallApiNum += int64(len(elems) - 1)
	d.stat.TotalCallApiNum += 1
	return key2Tasks, true
}

// 同一批不会拆分
func (d *Agg) run() {
	checkTicker := time.NewTicker(time.Duration(d.aggCfg.CheckIntervalMs) * time.Millisecond)
	statReportTicker := time.NewTicker(time.Second)
	for {
		select {
		case taskChunk := <-d.taskChunkCh:
			now := time.Now()
			d.lastRecvTaskChunkTs = now.Unix()
			d.stat.RecvTaskChunkNum++
			d.stat.TotalRecvDelay += now.Sub(taskChunk.SubmitTime).Microseconds()
			d.waitingTasksQueue.PushBack(taskChunk) //入队
			d.taskNum += len(taskChunk.Tasks)
			//判断下
			key2Tasks, get := d.tryGetTasks(true)
			if get {
				go d.mget(context.Background(), key2Tasks) //执行批量查询
				checkTicker.Stop()                         //重置ticker,聚合效果更好
				checkTicker = time.NewTicker(time.Duration(d.aggCfg.CheckIntervalMs) * time.Millisecond)
				d.aggCfg.DebugLog("id=%v,fetch taskChunk by tasksCh", d.id)
			}
		case <-checkTicker.C:
			//定时兜底/统计
			key2Tasks, get := d.tryGetTasks(false)
			if get {
				go d.mget(context.Background(), key2Tasks)
				d.aggCfg.DebugLog("id=%v,fetch taskChunk by ticker", d.id)
			}
		case <-statReportTicker.C:
			d.aggMng.aggStatChan <- d.stat
			d.stat = AggStat{}
		case <-d.ctx.Done():
			if !d.isClose {
				d.aggCfg.DebugLog("id=%v,ctx done", d.id)
				d.isClose = true
			} else {
				if d.lastRecvTaskChunkTs+10 < time.Now().Unix() {
					d.aggCfg.ErrLog("id=%v,ctx done,close", d.id)
					return
				}
			}
		}
	}
}

func (d *Agg) mget(ctx context.Context, key2Tasks map[interface{}][]Task) {
	keys := make([]interface{}, 0, len(key2Tasks))
	for k := range key2Tasks {
		keys = append(keys, k)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(d.aggCfg.MGetTimeoutMs)*time.Millisecond)
	defer cancel()
	key2Info, err := d.aggCfg.MGetFn(timeoutCtx, nil, keys)
	if err != nil {
		for _, tasks := range key2Tasks {
			for _, task := range tasks {
				task.ErrCh <- fmt.Errorf("disId=%v fetch err=%w", d.id, err)
				close(task.ErrCh)
			}
		}
		return
	}
	for key, tasks := range key2Tasks {
		info, ok := key2Info[key]
		if !ok {
			for _, task := range tasks {
				if !d.aggCfg.IgnoreItemResNotExist {
					task.ErrCh <- fmt.Errorf("disId=%v,not found key:%s", d.id, key)
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

// 底层被agg调度，聚合发送请求
// @agg 聚合器
// @keys 批量查询的key
func MGetWarp(ctx context.Context, agg *AggManager, keys []interface{}) (map[interface{}]interface{}, error) {
	tasks := make([]Task, 0, len(keys))
	for _, key := range keys {
		task := Task{
			TaskId: GenTaskId(),
			Key:    key,
			ResCh:  make(chan interface{}, 1),
			ErrCh:  make(chan error, 1),
		}
		tasks = append(tasks, task) //添加到任务列表
	}
	err := agg.Submit(ctx, tasks) //提交任务
	if err != nil {
		return nil, err
	}

	key2Res := make(map[interface{}]interface{})
	for _, task := range tasks {
		select {
		case res := <-task.ResCh: //获取结果
			if res != nil {
				key2Res[task.Key] = res
			}
		case err := <-task.ErrCh: //获取错误
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return key2Res, nil
}
