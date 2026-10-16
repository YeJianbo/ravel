package task

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskContinuesAfterExecutionError(t *testing.T) {
	for _, first := range []bool{false, true} {
		t.Run(map[bool]string{false: "periodic", true: "immediate"}[first], func(t *testing.T) {
			var calls atomic.Int32
			recovered := make(chan struct{}, 1)
			task := &Task{Interval: 10 * time.Millisecond, Execute: func(context.Context) error {
				if calls.Add(1) == 1 {
					return errors.New("temporary failure")
				}
				select {
				case recovered <- struct{}{}:
				default:
				}
				return nil
			}}
			if err := task.Start(first); err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			select {
			case <-recovered:
			case <-time.After(time.Second):
				t.Fatal("task did not recover after its first failure")
			}
		})
	}
}

func TestTaskCloseCancelsActiveExecutionWithoutOverlap(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	task := &Task{Interval: time.Second, Execute: func(ctx context.Context) error {
		calls.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
		return ctx.Err()
	}}
	if err := task.Start(true); err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}
	if err := task.ExecuteWithTimeout(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("active callback overlapped with another execution")
	}
	task.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("closing the task did not cancel its callback")
	}
}
