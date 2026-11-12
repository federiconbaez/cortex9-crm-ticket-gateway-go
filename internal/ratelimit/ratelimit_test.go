package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(10, 5) // 10 tok/s, burst 5
	l.now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		d := l.Allow("a")
		if !d.Allowed || d.Remaining != 4-i || d.Limit != 5 {
			t.Fatalf("i=%d %+v", i, d)
		}
	}
	d := l.Allow("a")
	if d.Allowed || d.RetryAfter != 100*time.Millisecond || d.Remaining != 0 {
		t.Fatalf("%+v", d)
	}
	if CeilSeconds(d.RetryAfter) != 1 || CeilSeconds(0) != 0 || CeilSeconds(1500*time.Millisecond) != 2 {
		t.Fatal("ceil")
	}
	// otro cliente no se ve afectado (aislamiento por tenant)
	if !l.Allow("b").Allowed {
		t.Fatal("b blocked")
	}
	now = now.Add(250 * time.Millisecond) // +2.5 tokens
	first, second, third := l.Allow("a").Allowed, l.Allow("a").Allowed, l.Allow("a").Allowed
	if !first || !second || third {
		t.Fatal("refill")
	}
	now = now.Add(time.Hour)
	d = l.Allow("a")
	if d.Remaining != 4 || d.Reset != 100*time.Millisecond {
		t.Fatalf("cap at burst: %+v", d)
	}
	if l.AllowN("a", 10).Allowed {
		t.Fatal("n>burst")
	}
	if l.Policy() != "5;w=1" {
		t.Fatal(l.Policy())
	}
}

func TestSweepAndZeroRate(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(1, 2)
	l.now = func() time.Time { return now }
	for i := 0; i < 100; i++ {
		l.Allow(fmt.Sprint(i))
	}
	if l.Len() != 100 {
		t.Fatal(l.Len())
	}
	if l.Sweep() != 0 {
		t.Fatal("swept active buckets")
	}
	now = now.Add(3 * time.Second)
	l.Allow("fresh")
	if n := l.Sweep(); n != 100 || l.Len() != 1 {
		t.Fatalf("swept %d len %d", n, l.Len())
	}
	z := New(0, 1)
	z.Allow("x")
	if d := z.Allow("x"); d.Allowed || d.RetryAfter != time.Hour {
		t.Fatalf("%+v", d)
	}
}

func TestConcurrentNeverExceedsBurst(t *testing.T) {
	l := New(0.0001, 50)
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("k").Allowed {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 50 {
		t.Fatalf("allowed %d, want 50", ok.Load())
	}
}
