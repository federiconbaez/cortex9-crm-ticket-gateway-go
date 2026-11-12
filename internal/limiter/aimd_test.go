package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestShedsAboveLimit(t *testing.T) {
	l := New(Config{Initial: 2, Min: 1, Max: 10, Target: time.Second})
	r1, ok1 := l.Acquire()
	r2, ok2 := l.Acquire()
	_, ok3 := l.Acquire()
	if !ok1 || !ok2 || ok3 || l.InFlight() != 2 {
		t.Fatal("limit not enforced")
	}
	r1(Ignore)
	r1(Success) // idempotente
	if l.InFlight() != 1 || l.Limit() != 2 {
		t.Fatal(l.InFlight(), l.Limit())
	}
	r2(Ignore)
}

func TestAdditiveIncreaseMultiplicativeDecrease(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(Config{Initial: 10, Min: 2, Max: 20, Target: 50 * time.Millisecond, Backoff: 0.5, Cooldown: time.Second})
	l.now = func() time.Time { return now }
	// 10 requests rápidas con el límite lleno -> +1 aprox.
	var rel []func(Outcome)
	for i := 0; i < 10; i++ {
		r, ok := l.Acquire()
		if !ok {
			t.Fatal("acquire")
		}
		rel = append(rel, r)
	}
	for _, r := range rel {
		r(Success)
	}
	if l.Limit() != 10 && l.Limit() != 11 {
		t.Fatalf("AI: %d", l.Limit())
	}
	got := l.Limit()
	// una lenta -> halve
	r, _ := l.Acquire()
	now = now.Add(100 * time.Millisecond)
	r(Success)
	if l.Limit() != got/2 {
		t.Fatalf("MD: %d -> %d", got, l.Limit())
	}
	// segunda señal dentro del cooldown: no baja de nuevo
	r, _ = l.Acquire()
	r(Dropped)
	if l.Limit() != got/2 {
		t.Fatal("cooldown ignored")
	}
	for i := 0; i < 10; i++ {
		now = now.Add(2 * time.Second)
		r, _ = l.Acquire()
		r(Dropped)
	}
	if l.Limit() != 2 {
		t.Fatalf("min: %d", l.Limit())
	}
	// sin uso del límite (inflight < limit/2) no crece
	l2 := New(Config{Initial: 10, Min: 1, Max: 100, Target: time.Second})
	for i := 0; i < 100; i++ {
		r, _ := l2.Acquire()
		r(Success)
	}
	if l2.Limit() != 10 {
		t.Fatalf("grew without utilisation: %d", l2.Limit())
	}
}

func TestDefaultsAndMax(t *testing.T) {
	l := New(Config{Initial: 0, Min: 0, Max: 0})
	if l.Limit() != 1 || l.cfg.Backoff != 0.9 {
		t.Fatal(l.Limit())
	}
	l = New(Config{Initial: 3, Min: 1, Max: 3, Target: time.Hour})
	for i := 0; i < 1000; i++ {
		var rs []func(Outcome)
		for j := 0; j < 3; j++ {
			r, _ := l.Acquire()
			rs = append(rs, r)
		}
		for _, r := range rs {
			r(Success)
		}
	}
	if l.Limit() != 3 {
		t.Fatalf("max exceeded: %d", l.Limit())
	}
}

func TestConcurrentInvariant(t *testing.T) {
	l := New(Config{Initial: 8, Min: 1, Max: 64, Target: time.Millisecond})
	var wg sync.WaitGroup
	var mu sync.Mutex
	maxSeen := 0
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, ok := l.Acquire()
			if !ok {
				return
			}
			mu.Lock()
			if f := l.InFlight(); f > maxSeen {
				maxSeen = f
			}
			mu.Unlock()
			if i%3 == 0 {
				r(Dropped)
			} else {
				r(Success)
			}
		}(i)
	}
	wg.Wait()
	if l.InFlight() != 0 || maxSeen > 64 {
		t.Fatal(l.InFlight(), maxSeen)
	}
}
