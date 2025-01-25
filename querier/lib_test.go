package querier

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// 基础查询器实现
type DemoQuerier struct {
	BaseQuerier
}

func (q *DemoQuerier) Query(ctx context.Context) error {
	for key := range q.pendingKeys {
		q.key2Res[key] = fmt.Sprintf("demo-%v", key)
	}
	return nil
}

// 错误查询器实现
type FaultQuerier struct {
	BaseQuerier
}

func (q *FaultQuerier) Query(ctx context.Context) error {
	return errors.New("intentional failure")
}

// 并发安全测试查询器
type ConcurrentQuerier struct {
	BaseQuerier
	mu sync.Mutex
}

func (q *ConcurrentQuerier) Query() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for key := range q.pendingKeys {
		q.key2Res[key] = true
	}
	return nil
}

func TestBaseQuerier(t *testing.T) {
	t.Run("Key deduplication", func(t *testing.T) {
		q := &BaseQuerier{}
		q.AddKeys([]interface{}{1, 1, 2}, false)
		if len(q.pendingKeys) != 2 {
			t.Errorf("Duplicate keys not filtered, got %d keys", len(q.pendingKeys))
		}
	})

	t.Run("Clear pending keys", func(t *testing.T) {
		q := &BaseQuerier{}
		q.AddKeys([]interface{}{1}, false)
		q.ClearPendingKeys()
		if q.NeedQuery() {
			t.Error("ClearPendingKeys failed")
		}
	})
}

func TestQuerierStore_BasicFlow(t *testing.T) {
	store := NewQuerier(context.Background())
	store.AddQuerier("demo", &DemoQuerier{})

	// 设置两步加载流程
	store.SetStepFuncs([]StepFunc{
		// 第一步：加载初始数据
		func(qs *QuerierStore) error {
			qs.AddKeys("demo", []interface{}{"a", "b"}, false)
			return nil
		},
		// 第二步：加载补充数据
		func(qs *QuerierStore) error {
			qs.AddKey("demo", "c", false)
			return nil
		},
	})

	if err := store.Exec(); err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	// 验证结果
	expected := map[interface{}]string{
		"a": "demo-a",
		"b": "demo-b",
		"c": "demo-c",
	}
	for k, v := range expected {
		if res, ok := store.Load("demo", k); !ok || res != v {
			t.Errorf("Key %v: expected %q, got %v", k, v, res)
		}
	}
}

func TestQuerierStore_ErrorFlows(t *testing.T) {
	t.Run("Propagate errors", func(t *testing.T) {
		store := NewQuerier(context.Background())
		store.AddQuerier("fault", &FaultQuerier{})

		store.SetStepFuncs([]StepFunc{
			func(qs *QuerierStore) error {
				qs.AddKey("fault", 1, false) // 不忽略错误
				return nil
			},
		})

		err := store.Exec()
		if err == nil || err.Error() != "exec querier fault err: intentional failure" {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("Ignore errors", func(t *testing.T) {
		store := NewQuerier(context.Background())
		store.AddQuerier("fault", &FaultQuerier{})

		store.SetStepFuncs([]StepFunc{
			func(qs *QuerierStore) error {
				qs.AddKey("fault", 1, true) // 忽略错误
				return nil
			},
		})

		if err := store.Exec(); err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})
}

func TestQuerierStore_MultiStep(t *testing.T) {
	store := NewQuerier(context.Background())
	store.AddQuerier("demo", &DemoQuerier{})

	stepTracker := make([]int, 0)
	store.SetStepFuncs([]StepFunc{
		func(qs *QuerierStore) error {
			stepTracker = append(stepTracker, 1)
			qs.AddKey("demo", "step1", false)
			return nil
		},
		func(qs *QuerierStore) error {
			stepTracker = append(stepTracker, 2)
			qs.AddKey("demo", "step2", false)
			return nil
		},
		func(qs *QuerierStore) error {
			stepTracker = append(stepTracker, 3)
			return nil
		},
	})

	if err := store.Exec(); err != nil {
		t.Fatal(err)
	}

	// 验证步骤执行顺序
	expectedSteps := []int{1, 2, 3}
	if fmt.Sprint(stepTracker) != fmt.Sprint(expectedSteps) {
		t.Errorf("Step execution order mismatch: got %v, want %v", stepTracker, expectedSteps)
	}

	// 验证各步骤添加的键
	for _, key := range []string{"step1", "step2"} {
		if _, ok := store.Load("demo", key); !ok {
			t.Errorf("Key %q not processed", key)
		}
	}
}

type DepQuerier1 struct {
	BaseQuerier
}

func (d *DepQuerier1) Query(ctx context.Context) error {
	for key := range d.pendingKeys {
		tk := key.(Key2Res)
		d.key2Res[key] = Key2Res{
			Id:  tk.Id,
			Val: fmt.Sprintf("dep1-%v", tk.Val),
		}
	}
	return nil
}

type DepQuerier2 struct {
	BaseQuerier
}

func (d *DepQuerier2) Query(ctx context.Context) error {
	for key := range d.pendingKeys {
		tk := key.(Key2Res)
		d.key2Res[key] = Key2Res{
			Id:  tk.Id,
			Val: fmt.Sprintf("dep2-%v", tk.Val),
		}
	}
	return nil
}

type DepQuerier3 struct {
	BaseQuerier
}

func (d *DepQuerier3) Query(ctx context.Context) error {
	for key := range d.pendingKeys {
		tk := key.(Key2Res)
		d.key2Res[key] = Key2Res{
			Id:  tk.Id,
			Val: fmt.Sprintf("dep3-%v", tk.Val),
		}
	}
	return nil
}

type Key2Res struct {
	Id  int
	Val string
}

func TestQuerierStore_Dependency(t *testing.T) {
	for i := 0; i < 100; i++ {
		store := NewQuerier(context.Background())
		store.AddQuerier("dep1", &DepQuerier1{})
		store.AddQuerier("dep2", &DepQuerier2{})
		store.AddQuerier("dep3", &DepQuerier3{})

		dep1Keys := func() []interface{} {
			keys := make([]interface{}, 0)
			for i := 0; i < 10; i++ {
				id := rand.Intn(10)
				keys = append(keys, Key2Res{
					Id:  id,
					Val: fmt.Sprint(id),
				})
			}
			return keys
		}()
		dep2Keys := make([]interface{}, 0)
		dep3Keys := make([]interface{}, 0)
		store.SetStepFuncs([]StepFunc{
			func(qs *QuerierStore) error {
				qs.AddKeys("dep1", dep1Keys, false)
				return nil
			},
			func(qs *QuerierStore) error {
				dep1Vals := qs.BatchLoad("dep1", dep1Keys)
				for _, v := range dep1Vals {
					dep2Keys = append(dep2Keys, v)
				}
				qs.AddKeys("dep2", dep2Keys, false)
				return nil
			},
			func(qs *QuerierStore) error {
				dep2Vals := qs.BatchLoad("dep2", dep2Keys)
				for _, v := range dep2Vals {
					dep3Keys = append(dep3Keys, v)
				}
				qs.AddKeys("dep3", dep3Keys, false)
				return nil
			},
		})
		if err := store.Exec(); err != nil {
			t.Fatalf("Exec failed: %v", err)
		}
		dep3Vals := store.BatchLoad("dep3", dep3Keys)
		id2Dep3Vals := make(map[int]string)
		for _, v := range dep3Vals {
			tk := v.(Key2Res)
			id2Dep3Vals[tk.Id] = tk.Val
		}
		for _, k := range dep1Keys {
			tk := k.(Key2Res)
			if id2Dep3Vals[tk.Id] != fmt.Sprintf("dep3-dep2-dep1-%v", tk.Val) {
				t.Errorf("Key %v: expected %q, got %v", tk, fmt.Sprintf("dep3-dep2-dep1-%v", tk.Val), id2Dep3Vals[tk.Id])
			}
		}
		t.Log(id2Dep3Vals)
	}
}
