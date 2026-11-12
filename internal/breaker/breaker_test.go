package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time      { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) add(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

var errX = errors.New("x")

func newB(ck *fakeClock) (*Breaker, *[]string) {
	b := New("crm", Config{Window: 10 * time.Second, Buckets: 10, MinRequests: 10, FailureRatio: 0.5, OpenTimeout: 5 * time.Second, HalfOpenProbes: 2})
	b.now = ck.now
	var log []string
	var mu sync.Mutex
	b.OnStateChange = func(n string, from, to State) {
		mu.Lock()
		log = append(log, from.String()+">"+to.String())
		mu.Unlock()
	}
	return b, &log
}

func TestFullCycle(t *testing.T) {
	ck := &fakeClock{t: time.Unix(1000, 0)}
	b, log := newB(ck)
	if b.Name() != "crm" {
		t.Fatal()
	}
	// 9 fallas: debajo del mínimo, sigue cerrado
	for i := 0; i < 9; i++ {
		_ = b.Do(func() error { return errX })
	}
	if b.State() != Closed {
		t.Fatal("opened below MinRequests")
	}
	_ = b.Do(func() error { return errX })
	if b.State() != Open {
		t.Fatal("should open")
	}
	if err := b.Do(func() error { t.Fatal("called while open"); return nil }); !errors.Is(err, ErrOpen) {
		t.Fatal(err)
	}
	ck.add(5 * time.Second)
	if b.State() != HalfOpen {
		t.Fatal("should report half-open")
	}
	d1, err1 := b.Allow()
	d2, err2 := b.Allow()
	_, err3 := b.Allow()
	if err1 != nil || err2 != nil || !errors.Is(err3, ErrOpen) {
		t.Fatal("half-open probes", err1, err2, err3)
	}
	d1(true)
	d1(false) // done idempotente: se ignora
	d2(false)
	if b.State() != Open {
		t.Fatal("probe failure should reopen")
	}
	ck.add(5 * time.Second)
	_ = b.Do(func() error { return nil })
	_ = b.Do(func() error { return nil })
	if b.State() != Closed {
		t.Fatal("should close", b.State())
	}
	want := []string{"closed>open", "open>half_open", "half_open>open", "open>half_open", "half_open>closed"}
	if len(*log) != len(want) {
		t.Fatalf("%v", *log)
	}
	for i := range want {
		if (*log)[i] != want[i] {
			t.Fatalf("%v", *log)
		}
	}
}

func TestWindowSlides(t *testing.T) {
	ck := &fakeClock{t: time.Unix(1000, 0)}
	b, _ := newB(ck)
	for i := 0; i < 8; i++ {
		_ = b.Do(func() error { return errX })
	}
	ck.add(11 * time.Second) // las fallas viejas salen de la ventana
	for i := 0; i < 8; i++ {
		_ = b.Do(func() error { return nil })
	}
	_ = b.Do(func() error { return errX })
	_ = b.Do(func() error { return errX })
	if b.State() != Closed {
		t.Fatal("old failures should have expired")
	}
	// resultado tardío en open se ignora
	ck2 := &fakeClock{t: time.Unix(0, 0)}
	b2, _ := newB(ck2)
	late, _ := b2.Allow()
	for i := 0; i < 10; i++ {
		_ = b2.Do(func() error { return errX })
	}
	late(true)
	if b2.State() != Open {
		t.Fatal()
	}
	if Open.String() != "open" || HalfOpen.String() != "half_open" || Closed.String() != "closed" {
		t.Fatal()
	}
	d := New("d", Config{})
	if d.cfg.Buckets != 10 || d.cfg.Window != 10*time.Second || d.cfg.HalfOpenProbes != 1 {
		t.Fatal("defaults")
	}
}

func TestConcurrent(t *testing.T) {
	b := New("c", Config{MinRequests: 20, FailureRatio: 0.5, OpenTimeout: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = b.Do(func() error {
				if i%2 == 0 {
					return errX
				}
				return nil
			})
			_ = b.State()
		}(i)
	}
	wg.Wait()
}
