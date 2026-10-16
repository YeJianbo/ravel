package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Name      string
	Interval  time.Duration
	Execute   func(context.Context) error
	Access    sync.RWMutex
	Running   bool
	ReloadCh  chan struct{}
	Stop      chan struct{}
	executing atomic.Bool
}

func (t *Task) Start(first bool) error {
	t.Access.Lock()
	defer t.Access.Unlock()
	if t.Running {
		return nil
	}
	if t.Interval <= 0 {
		return errors.New("task interval must be positive")
	}
	if t.Execute == nil {
		return errors.New("task callback is required")
	}
	t.Running = true
	stop := make(chan struct{})
	t.Stop = stop
	go func() {
		defer func() {
			t.Access.Lock()
			defer t.Access.Unlock()
			if t.Stop == stop && t.Running {
				t.Running = false
				close(stop)
			}
		}()
		timer := time.NewTimer(t.Interval)
		defer timer.Stop()
		if first {
			if err := t.executeWithTimeout(stop); err != nil {
				log.Errorf("Task %s execution error: %v", t.Name, err)
			}
		}

		for {
			timer.Reset(t.Interval)
			select {
			case <-timer.C:
				// continue
			case <-stop:
				return
			}

			if err := t.executeWithTimeout(stop); err != nil {
				log.Errorf("Task %s execution error: %v", t.Name, err)
			}
		}
	}()

	return nil
}

func (t *Task) ExecuteWithTimeout() error {
	t.Access.RLock()
	stop := t.Stop
	t.Access.RUnlock()
	return t.executeWithTimeout(stop)
}

func (t *Task) executeWithTimeout(stop <-chan struct{}) error {
	select {
	case <-stop:
		return nil
	default:
	}
	if !t.executing.CompareAndSwap(false, true) {
		return nil
	}
	timeout := 5 * time.Minute
	if t.Interval < time.Minute {
		timeout = 5 * t.Interval
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- t.executeOnce(ctx)
	}()

	select {
	case <-stop:
		return nil
	case <-ctx.Done():
		select {
		case <-stop:
			return nil
		default:
		}
		log.Errorf("Task %s execution timed out, reloading", t.Name)
		if t.ReloadCh != nil {
			select {
			case t.ReloadCh <- struct{}{}:
			default:
			}
		} else {
			log.Panic("Reload failed")
		}
		return nil
	case err := <-done:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}
}

func (t *Task) executeOnce(ctx context.Context) error {
	defer t.executing.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	return t.Execute(ctx)
}

func (t *Task) safeStop() {
	t.Access.Lock()
	if t.Running {
		t.Running = false
		close(t.Stop)
	}
	t.Access.Unlock()
}

func (t *Task) Close() {
	t.safeStop()
	log.Warningf("Task %s stopped", t.Name)
}
