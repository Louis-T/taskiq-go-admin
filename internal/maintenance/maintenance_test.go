package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunBatchesStopsOnShortBatch(t *testing.T) {
	remaining := int64(23)
	var calls []int64
	total, err := RunBatches(context.Background(), BatchConfig{Size: 10}, func(_ context.Context, limit int) (int64, error) {
		n := min(remaining, int64(limit))
		remaining -= n
		calls = append(calls, n)
		return n, nil
	})
	if err != nil || total != 23 || len(calls) != 3 {
		t.Fatalf("total=%d calls=%v err=%v", total, calls, err)
	}
}

func TestRunBatchesExactMultipleNeedsOneEmptyCall(t *testing.T) {
	remaining := int64(20)
	calls := 0
	total, _ := RunBatches(context.Background(), BatchConfig{Size: 10}, func(_ context.Context, limit int) (int64, error) {
		calls++
		n := min(remaining, int64(limit))
		remaining -= n
		return n, nil
	})
	if total != 20 || calls != 3 {
		t.Fatalf("total=%d calls=%d", total, calls)
	}
}

func TestRunBatchesStopsOnError(t *testing.T) {
	boom := errors.New("boom")
	total, err := RunBatches(context.Background(), BatchConfig{Size: 10}, func(context.Context, int) (int64, error) {
		return 4, boom
	})
	if !errors.Is(err, boom) || total != 4 {
		t.Fatalf("total=%d err=%v", total, err)
	}
}

func TestRunBatchesHonoursCancellationDuringPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := RunBatches(ctx, BatchConfig{Size: 1, Pause: time.Hour}, func(context.Context, int) (int64, error) {
			calls++
			return 1, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v", err)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunBatches did not stop")
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}
