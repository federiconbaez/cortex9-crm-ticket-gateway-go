package downstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSim(t *testing.T) {
	s := New("crm", 7, Chaos{})
	if s.Name() != "crm" || s.Call(context.Background()) != nil {
		t.Fatal()
	}
	s.SetChaos(Chaos{ErrorRate: 1})
	if !errors.Is(s.Call(context.Background()), ErrInjected) || s.Chaos().ErrorRate != 1 {
		t.Fatal()
	}
	s.SetChaos(Chaos{Latency: 50 * time.Millisecond, Jitter: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if !errors.Is(s.Call(ctx), context.DeadlineExceeded) {
		t.Fatal("ctx")
	}
	s.SetChaos(Chaos{Jitter: time.Millisecond})
	if s.Call(context.Background()) != nil {
		t.Fatal()
	}
	cctx, c := context.WithCancel(context.Background())
	c()
	s.SetChaos(Chaos{})
	if s.Call(cctx) == nil {
		t.Fatal("cancelled ctx")
	}
	tot, failed := s.Calls()
	if tot != 5 || failed != 3 {
		t.Fatal(tot, failed)
	}
	// determinismo: misma semilla => misma secuencia de fallas
	a, b := New("a", 1, Chaos{ErrorRate: 0.5}), New("b", 1, Chaos{ErrorRate: 0.5})
	for i := 0; i < 100; i++ {
		if (a.Call(context.Background()) == nil) != (b.Call(context.Background()) == nil) {
			t.Fatal("not deterministic")
		}
	}
}
