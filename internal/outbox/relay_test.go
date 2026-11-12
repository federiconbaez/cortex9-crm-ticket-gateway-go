package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/queue"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/store"
)

func fill(t *testing.T, s *store.Store, n int) {
	for i := 0; i < n; i++ {
		if err := s.WithTx(context.Background(), func(tx *store.Tx) error {
			tx.AppendOutbox(store.OutboxEvent{ID: fmt.Sprint("e", i)})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFlushOrderAndBlocking(t *testing.T) {
	s := store.New()
	fill(t, s, 5)
	var got []string
	full := false
	var flushes [][2]int
	r := NewRelay(s, func(e store.OutboxEvent) error {
		if full && e.ID == "e3" {
			return queue.ErrFull
		}
		if e.ID == "e1" {
			return queue.ErrDuplicate // ya estaba: cuenta como publicado
		}
		got = append(got, e.ID)
		return nil
	}, time.Hour, 10)
	r.OnFlush = func(p, f int) { flushes = append(flushes, [2]int{p, f}) }
	full = true
	n, blocked := r.Flush()
	if n != 3 || !blocked || s.OutboxLen() != 2 {
		t.Fatalf("n=%d blocked=%v pending=%d", n, blocked, s.OutboxLen())
	}
	full = false
	n, blocked = r.Flush()
	if n != 2 || blocked || s.OutboxLen() != 0 {
		t.Fatal(n, blocked)
	}
	want := []string{"e0", "e2", "e3", "e4"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatal(got)
		}
	}
	if len(flushes) != 2 || flushes[0] != [2]int{3, 1} {
		t.Fatal(flushes)
	}
	r.Flush() // vacío: no llama OnFlush
	if len(flushes) != 2 {
		t.Fatal()
	}
	d := NewRelay(s, nil, 0, 0)
	if d.Batch != 256 || d.Interval != 50*time.Millisecond {
		t.Fatal("defaults")
	}
}

func TestLoopKickAndStopDrains(t *testing.T) {
	s := store.New()
	var mu sync.Mutex
	var count int
	r := NewRelay(s, func(e store.OutboxEvent) error { mu.Lock(); count++; mu.Unlock(); return nil }, time.Hour, 2)
	r.Start()
	fill(t, s, 7)
	r.Kick()
	r.Kick()
	deadline := time.Now().Add(2 * time.Second)
	for s.OutboxLen() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.OutboxLen() != 0 {
		t.Fatal("kick did not flush in batches")
	}
	fill(t, s, 3) // sin kick: Stop debe drenar
	if left := r.Stop(context.Background()); left != 0 {
		t.Fatal(left)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 10 {
		t.Fatal(count)
	}
}

func TestStopRespectsDeadline(t *testing.T) {
	s := store.New()
	fill(t, s, 3)
	var calls atomic.Int32
	r := NewRelay(s, func(store.OutboxEvent) error { calls.Add(1); return errors.New("broker down") }, time.Millisecond, 10)
	r.Start()
	time.Sleep(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if left := r.Stop(ctx); left != 3 {
		t.Fatalf("left=%d", left)
	}
	if calls.Load() == 0 {
		t.Fatal()
	}
	// Stop sin Start
	r2 := NewRelay(store.New(), nil, 0, 0)
	if r2.Stop(context.Background()) != 0 {
		t.Fatal()
	}
}
