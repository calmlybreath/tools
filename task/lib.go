package task

import "context"

type result struct {
	data interface{}
	err  error
}

type TaskFunc func(ctx context.Context) (interface{}, error)

type Task struct {
	ctx        context.Context
	resultChan chan result
}

// start a task
func Start(ctx context.Context, task TaskFunc) *Task {
	resultChan := make(chan result, 1)
	go func() {
		data, err := task(ctx)
		resultChan <- result{data, err}
		close(resultChan)
	}()
	return &Task{
		ctx:        ctx,
		resultChan: resultChan,
	}
}

// await
func (t *Task) Await() (interface{}, error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	case result := <-t.resultChan:
		return result.data, result.err
	}
}
