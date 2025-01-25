package task

import (
	"context"
	"testing"
	"time"
)

func TestTask_Await(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	task := Start(ctx, func(ctx context.Context) (interface{}, error) {
		time.Sleep(1 * time.Second)
		return 1, nil
	})
	t.Logf("task: %v", task)
	data, err := task.Await()
	if err != nil {
		t.Fatal(err)
	}
	if data.(int) != 1 {
		t.Fatal("data is not 1")
	}
}
