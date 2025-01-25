package querier

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type IQuerier interface {
	AddKeys(keys []interface{}, ignoreErr bool)
	Load(key interface{}) (interface{}, bool)
	BatchLoad(keys []interface{}) map[interface{}]interface{}
	NeedQuery() bool
	ClearPendingKeys()
	IgnoreErr() bool

	Query(ctx context.Context) error //执行加载，继承BaseQuerier的只需要实现这个方法
}

type BaseQuerier struct {
	pendingKeys map[interface{}]struct{}
	key2Res     map[interface{}]interface{}
	ignoreErr   bool
}

func (b *BaseQuerier) AddKeys(keys []interface{}, ignoreErr bool) {
	if b.pendingKeys == nil {
		b.pendingKeys = make(map[interface{}]struct{})
	}
	if b.key2Res == nil {
		b.key2Res = make(map[interface{}]interface{})
	}
	for _, key := range keys {
		if _, ok := b.key2Res[key]; ok {
			continue
		}
		b.pendingKeys[key] = struct{}{}
	}
	b.ignoreErr = ignoreErr
}
func (b *BaseQuerier) Load(key interface{}) (interface{}, bool) {
	if b.key2Res == nil {
		return nil, false
	}
	res, ok := b.key2Res[key]
	return res, ok
}
func (b *BaseQuerier) BatchLoad(keys []interface{}) map[interface{}]interface{} {
	res := make(map[interface{}]interface{})
	for _, key := range keys {
		if r, ok := b.key2Res[key]; ok {
			res[key] = r
		}
	}
	return res
}

func (b *BaseQuerier) NeedQuery() bool {
	return len(b.pendingKeys) > 0
}
func (b *BaseQuerier) IgnoreErr() bool {
	return b.ignoreErr
}
func (b *BaseQuerier) ClearPendingKeys() {
	b.pendingKeys = nil
}

type StepFunc func(q *QuerierStore) error

type QuerierStore struct {
	ctx       context.Context
	queriers  map[string]IQuerier
	stepFuncs []StepFunc
}

func NewQuerier(ctx context.Context) *QuerierStore {
	return &QuerierStore{
		ctx:      ctx,
		queriers: make(map[string]IQuerier),
	}
}
func (q *QuerierStore) AddQuerier(name string, querier IQuerier) {
	q.queriers[name] = querier
}

func (q *QuerierStore) SetStepFuncs(stepFuncs []StepFunc) {
	q.stepFuncs = stepFuncs
}

func (q *QuerierStore) Load(name string, key interface{}) (interface{}, bool) {
	querier, ok := q.queriers[name]
	if !ok {
		return nil, false
	}
	return querier.Load(key)
}
func (q *QuerierStore) BatchLoad(name string, keys []interface{}) map[interface{}]interface{} {
	querier, ok := q.queriers[name]
	if !ok {
		return nil
	}
	return querier.BatchLoad(keys)
}

func (q *QuerierStore) AddKey(name string, key interface{}, ignoreErr bool) {
	querier, ok := q.queriers[name]
	if !ok {
		return
	}
	querier.AddKeys([]interface{}{key}, ignoreErr)
}
func (q *QuerierStore) AddKeys(name string, keys []interface{}, ignoreErr bool) {
	querier, ok := q.queriers[name]
	if !ok {
		return
	}
	querier.AddKeys(keys, ignoreErr)
}

func (q *QuerierStore) callStepFunc(idx int) error {
	if len(q.stepFuncs) == 0 {
		panic("step funcs is empty")
	}
	if len(q.stepFuncs) <= idx {
		return nil
	}
	stepFunc := q.stepFuncs[idx]
	return stepFunc(q)
}

// 并发
func (q *QuerierStore) Exec() error {
	for idx := 0; idx < len(q.stepFuncs); idx++ {
		err := q.callStepFunc(idx)
		if err != nil {
			return err
		}
		//并发执行
		wg := errgroup.Group{}
		for tmpName, tmpQuerier := range q.queriers {
			if !tmpQuerier.NeedQuery() {
				continue
			}
			name := tmpName
			querier := tmpQuerier
			wg.Go(func() error {
				execErr := querier.Query(q.ctx)
				if execErr != nil && !querier.IgnoreErr() {
					return fmt.Errorf("exec querier %s err: %w", name, execErr)
				}
				querier.ClearPendingKeys()
				return nil
			})
		}
		waitErr := wg.Wait()
		if waitErr != nil {
			return waitErr
		}
	}
	return nil
}
