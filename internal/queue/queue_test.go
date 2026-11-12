package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func cfg() Config {
	return Config{Workers: 4, Capacity: 100, MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, HandlerTimeout: time.Second}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	q := New(cfg())
	var attempts atomic.Int32
	var events sync.Map
	q.OnEvent = func(e Event, m Message, _ time.Duration) {
		v, _ := events.LoadOrStore(e, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
	}
	q.Start(func(ctx context.Context, m Message) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err := q.Enqueue(Message{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { v, ok := events.Load(EvProcessed); return ok && v.(*atomic.Int32).Load() == 1 })
	if attempts.Load() != 3 {
		t.Fatal(attempts.Load())
	}
	v, _ := events.Load(EvRetry)
	if v.(*atomic.Int32).Load() != 2 {
		t.Fatal("retries")
	}
	// consumidor idempotente: el mismo ID ya procesado no se reencola
	if err := q.Enqueue(Message{ID: "a"}); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
	if q.Shutdown(context.Background()) != 0 {
		t.Fatal("left work")
	}
	if err := q.Enqueue(Message{ID: "b"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	q.Shutdown(context.Background()) // idempotente
}

func TestDLQAndRedrive(t *testing.T) {
	q := New(cfg())
	var fail atomic.Bool
	fail.Store(true)
	q.Start(func(ctx context.Context, m Message) error {
		if m.ID == "perm" {
			return Permanent(errors.New("bad payload"))
		}
		if m.ID == "panic" {
			panic("boom")
		}
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	})
	_ = q.Enqueue(Message{ID: "retry"})
	_ = q.Enqueue(Message{ID: "perm"})
	_ = q.Enqueue(Message{ID: "panic"})
	waitFor(t, func() bool { return q.Stats().DLQ == 3 })
	reasons := map[string]string{}
	for _, d := range q.DLQ() {
		reasons[d.Message.ID] = d.Reason
		if d.Message.ID == "retry" && d.Message.Attempt != 3 {
			t.Fatal("attempts", d.Message.Attempt)
		}
	}
	if reasons["retry"] != "max_attempts" || reasons["perm"] != "permanent" || reasons["panic"] != "permanent" {
		t.Fatal(reasons)
	}
	fail.Store(false)
	if n := q.Redrive(); n != 3 {
		t.Fatal(n)
	}
	waitFor(t, func() bool { s := q.Stats(); return s.DLQ == 2 && s.Ready+s.Delayed+s.InFlight == 0 })
	q.Shutdown(context.Background())
	if !IsPermanent(Permanent(errors.New("x"))) || IsPermanent(errors.New("x")) || Permanent(errors.New("x")).Error() != "permanent: x" {
		t.Fatal()
	}
}

func TestPriorityAndFairness(t *testing.T) {
	c := cfg()
	c.Workers = 1
	c.FairEvery = 4
	q := New(c)
	// encolamos antes de arrancar: 6 P4 viejos y luego 6 P1
	for i := 0; i < 6; i++ {
		_ = q.Enqueue(Message{ID: fmt.Sprint("p4-", i), Rank: 3})
	}
	for i := 0; i < 6; i++ {
		_ = q.Enqueue(Message{ID: fmt.Sprint("p1-", i), Rank: 0})
	}
	var mu sync.Mutex
	var order []string
	q.Start(func(ctx context.Context, m Message) error {
		mu.Lock()
		order = append(order, m.ID)
		mu.Unlock()
		return nil
	})
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 12 })
	q.Shutdown(context.Background())
	// los 3 primeros son P1; el 4to pop es "fair" y toma el P4 más viejo.
	want := []string{"p1-0", "p1-1", "p1-2", "p4-0", "p1-3", "p1-4", "p1-5", "p4-1"}
	for i, w := range want {
		if order[i] != w {
			t.Fatalf("order %v", order)
		}
	}
}

func TestBackpressureAndDuplicates(t *testing.T) {
	c := cfg()
	c.Capacity = 2
	q := New(c)
	var seen []Event
	q.OnEvent = func(e Event, m Message, _ time.Duration) { seen = append(seen, e) }
	_ = q.Enqueue(Message{ID: "1"})
	if err := q.Enqueue(Message{ID: "1"}); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
	_ = q.Enqueue(Message{ID: "2"})
	if err := q.Enqueue(Message{ID: "3"}); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != EvDuplicate {
		t.Fatal(seen)
	}
	// Redrive con cola llena: los mensajes vuelven a la DLQ.
	q.dlq = []DeadLetter{{Message: Message{ID: "x"}}}
	if q.Redrive() != 0 || len(q.DLQ()) != 1 {
		t.Fatal("redrive into full queue")
	}
}

func TestBackoffBounds(t *testing.T) {
	q := New(Config{BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second})
	q.SetSeed(42, 7)
	for attempt := 1; attempt <= 40; attempt++ {
		ceil := 100 * time.Millisecond << (attempt - 1)
		if ceil > time.Second || attempt > 10 {
			ceil = time.Second
		}
		for i := 0; i < 200; i++ {
			d := q.Backoff(attempt)
			if d < 0 || d >= ceil {
				t.Fatalf("attempt %d: %v outside [0,%v)", attempt, d, ceil)
			}
		}
	}
	// reproducible con la misma semilla
	a, b := New(cfg()), New(cfg())
	a.SetSeed(1, 1)
	b.SetSeed(1, 1)
	for i := 1; i < 10; i++ {
		if a.Backoff(i) != b.Backoff(i) {
			t.Fatal("not deterministic")
		}
	}
	d := New(Config{})
	if d.cfg.Workers != 4 || d.cfg.MaxAttempts != 5 || d.cfg.MaxBackoff != 30*time.Second || d.cfg.FairEvery != 8 {
		t.Fatal("defaults")
	}
}

func TestShutdownDrainsDelayedRetries(t *testing.T) {
	c := cfg()
	c.BaseBackoff = 20 * time.Millisecond
	c.MaxBackoff = 20 * time.Millisecond
	q := New(c)
	var calls atomic.Int32
	q.Start(func(ctx context.Context, m Message) error {
		if calls.Add(1) == 1 {
			return errors.New("once")
		}
		return nil
	})
	_ = q.Enqueue(Message{ID: "a"})
	waitFor(t, func() bool { return q.Stats().Delayed == 1 })
	if left := q.Shutdown(context.Background()); left != 0 || calls.Load() != 2 {
		t.Fatalf("left=%d calls=%d", left, calls.Load())
	}
}

func TestShutdownDeadlineCancelsHandlers(t *testing.T) {
	q := New(cfg())
	started := make(chan struct{})
	q.Start(func(ctx context.Context, m Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	_ = q.Enqueue(Message{ID: "slow"})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	left := q.Shutdown(ctx)
	if left != 1 { // quedó programado para reintento, no se perdió en silencio
		t.Fatalf("left=%d", left)
	}
}

func TestSweepProcessed(t *testing.T) {
	now := time.Unix(0, 0)
	q := New(Config{DedupeTTL: time.Minute})
	q.now = func() time.Time { return now }
	q.processed["a"] = now
	now = now.Add(2 * time.Minute)
	if q.SweepProcessed() != 1 {
		t.Fatal()
	}
	if err := q.Enqueue(Message{ID: "a"}); err != nil {
		t.Fatal(err)
	}
}
