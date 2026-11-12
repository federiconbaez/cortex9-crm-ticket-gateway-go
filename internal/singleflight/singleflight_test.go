package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollapses(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var shared atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err, sh := g.Do("k", func() (int, error) {
				calls.Add(1)
				time.Sleep(30 * time.Millisecond)
				return 7, nil
			})
			if v != 7 || err != nil {
				t.Errorf("v=%d err=%v", v, err)
			}
			if sh {
				shared.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fn ran %d times", calls.Load())
	}
	if shared.Load() != 100 {
		t.Fatalf("shared=%d", shared.Load())
	}
	if g.InFlight("k") {
		t.Fatal("still in flight")
	}
}

func TestErrorAndPanic(t *testing.T) {
	var g Group[int, string]
	boom := errors.New("x")
	if _, err, sh := g.Do(1, func() (string, error) { return "", boom }); !errors.Is(err, boom) || sh {
		t.Fatal(err)
	}
	_, err, _ := g.Do(2, func() (string, error) { panic("kaboom") })
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Error() == "" {
		t.Fatalf("want panic error, got %v", err)
	}
	// la clave no queda trabada después del panic
	if v, err, _ := g.Do(2, func() (string, error) { return "ok", nil }); v != "ok" || err != nil {
		t.Fatal(v, err)
	}
}
