package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestStampedeCollapsed(t *testing.T) {
	c := New[string, int](Config{TTL: time.Minute}, StringHash)
	var loads atomic.Int32
	load := func(ctx context.Context) (int, error) {
		loads.Add(1)
		time.Sleep(20 * time.Millisecond)
		return 1, nil
	}
	var wg sync.WaitGroup
	results := make(chan Result, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, r, err := c.Get(context.Background(), "k", load)
			if err != nil {
				t.Error(err)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	if loads.Load() != 1 {
		t.Fatalf("stampede: %d loads", loads.Load())
	}
	count := map[Result]int{}
	for r := range results {
		count[r]++
	}
	if count[Miss]+count[Shared]+count[Hit] != 200 || count[Shared] == 0 {
		t.Fatalf("results %v", count)
	}
}

func TestTTLAndInvalidate(t *testing.T) {
	ck := &clock{t: time.Unix(0, 0)}
	c := New[string, int](Config{TTL: 10 * time.Second}, StringHash)
	c.now = ck.now
	var n atomic.Int32
	load := func(context.Context) (int, error) { return int(n.Add(1)), nil }
	v, r, _ := c.Get(context.Background(), "k", load)
	if v != 1 || r != Miss {
		t.Fatal(v, r)
	}
	v, r, _ = c.Get(context.Background(), "k", load)
	if v != 1 || r != Hit {
		t.Fatal(v, r)
	}
	ck.add(11 * time.Second)
	v, _, _ = c.Get(context.Background(), "k", load)
	if v != 2 {
		t.Fatal("ttl not honoured", v)
	}
	c.Invalidate("k")
	v, _, _ = c.Get(context.Background(), "k", load)
	if v != 3 || c.Len() != 1 {
		t.Fatal("invalidate", v)
	}
}

func TestXFetchProbabilityGrowsNearExpiry(t *testing.T) {
	ck := &clock{t: time.Unix(0, 0)}
	c := New[string, int](Config{TTL: 10 * time.Second, Beta: 1}, StringHash)
	c.now = ck.now
	e := &entry[int]{delta: time.Second, expiry: ck.now().Add(10 * time.Second)}
	frac := func(remaining time.Duration) float64 {
		hits := 0
		now := e.expiry.Add(-remaining)
		for i := 0; i < 20000; i++ {
			if c.shouldEarlyRefresh(e, now) {
				hits++
			}
		}
		return float64(hits) / 20000
	}
	// P(refresh) = P(Exp(1)*delta >= remaining) = exp(-remaining/delta)
	far, mid, near := frac(5*time.Second), frac(time.Second), frac(100*time.Millisecond)
	if !(far < mid && mid < near) {
		t.Fatalf("not monotonic: %v %v %v", far, mid, near)
	}
	if far > 0.02 || mid < 0.3 || mid > 0.45 || near < 0.85 {
		t.Fatalf("unexpected probabilities far=%v mid=%v near=%v", far, mid, near)
	}
	c.cfg.Beta = 0
	if c.shouldEarlyRefresh(e, e.expiry.Add(-time.Millisecond)) {
		t.Fatal("beta=0 must disable")
	}
	c.cfg.Beta = 1
	c.rnd = func() float64 { return 0 }
	if !c.shouldEarlyRefresh(e, ck.now()) {
		t.Fatal("rnd=0 means refresh")
	}
}

func TestEarlyRefreshServesStaleValueAndRefreshesOnce(t *testing.T) {
	ck := &clock{t: time.Unix(0, 0)}
	c := New[string, int](Config{TTL: 10 * time.Second, Beta: 1}, StringHash)
	c.now = ck.now
	c.rnd = func() float64 { return 1e-9 } // fuerza refresh temprano
	var res []Result
	var mu sync.Mutex
	c.OnResult = func(r Result) { mu.Lock(); res = append(res, r); mu.Unlock() }
	var n atomic.Int32
	load := func(context.Context) (int, error) { ck.add(time.Second); return int(n.Add(1)), nil }
	_, _, _ = c.Get(context.Background(), "k", load)
	v, r, _ := c.Get(context.Background(), "k", load)
	if v != 1 || r != EarlyRefresh {
		t.Fatal(v, r)
	}
	c.Wait()
	c.rnd = func() float64 { return 0.999999 }
	v, r, _ = c.Get(context.Background(), "k", load)
	if v != 2 || r != Hit {
		t.Fatal("refresh not stored", v, r)
	}
	if len(res) != 3 {
		t.Fatal(res)
	}
}

func TestLoadErrorNotCachedAndCallerCancel(t *testing.T) {
	c := New[string, int](Config{TTL: time.Minute, LoadTimeout: 200 * time.Millisecond}, StringHash)
	boom := errors.New("db down")
	if _, _, err := c.Get(context.Background(), "k", func(context.Context) (int, error) { return 0, boom }); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if c.Len() != 0 {
		t.Fatal("error cached")
	}
	// Un llamador que cancela no aborta la carga compartida.
	release := make(chan struct{})
	var loadErr atomic.Value
	slow := func(ctx context.Context) (int, error) {
		select {
		case <-release:
			return 9, nil
		case <-ctx.Done():
			loadErr.Store(ctx.Err())
			return 0, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := c.Get(ctx, "s", slow); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	v, _, err := c.Get(context.Background(), "s", slow)
	if err != nil || v != 9 || loadErr.Load() != nil {
		t.Fatal(v, err, loadErr.Load())
	}
}

func TestBoundedSize(t *testing.T) {
	c := New[string, int](Config{TTL: time.Minute, MaxEntries: 64}, StringHash)
	for i := 0; i < 10000; i++ {
		c.Set(fmt.Sprint(i), i, 0)
	}
	if c.Len() > 64+shards {
		t.Fatalf("unbounded: %d", c.Len())
	}
	c2 := New[string, int](Config{}, StringHash)
	if c2.cfg.MaxEntries != 10000 || c2.cfg.LoadTimeout != time.Second {
		t.Fatal("defaults")
	}
}
