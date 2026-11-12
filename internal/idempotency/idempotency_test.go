package idempotency

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"
)

func TestLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	s := New(time.Minute, 5*time.Second)
	s.now = func() time.Time { return now }
	fp := FingerprintOf("POST", "/v1/tickets", []byte(`{"a":1}`))
	other := FingerprintOf("POST", "/v1/tickets", []byte(`{"a":2}`))
	if fp == other || fp == FingerprintOf("PATCH", "/v1/tickets", []byte(`{"a":1}`)) {
		t.Fatal("fingerprint collision")
	}
	r := s.Begin("k", fp)
	if r.Outcome != Started || r.Token == 0 {
		t.Fatal(r)
	}
	tok := r.Token
	in := s.Begin("k", fp)
	if in.Outcome != InFlight || in.Ready == nil {
		t.Fatal(in)
	}
	if o := s.Begin("k", other).Outcome; o != Mismatch {
		t.Fatal(o)
	}
	if s.Complete("k", tok+99, Response{Status: 500}) {
		t.Fatal("foreign token accepted")
	}
	if !s.Complete("k", tok, Response{Status: 201, Body: []byte("x")}) {
		t.Fatal("owner rejected")
	}
	select {
	case <-in.Ready:
	default:
		t.Fatal("waiters not released")
	}
	rp := s.Begin("k", fp)
	if rp.Outcome != Replay || rp.Response.Status != 201 || string(rp.Response.Body) != "x" {
		t.Fatal(rp)
	}
	if s.Complete("k", tok, Response{Status: 999}) || s.Abort("k", tok) {
		t.Fatal("completed entry mutated")
	}
	if s.Begin("k", fp).Response.Status != 201 {
		t.Fatal("overwritten")
	}
	now = now.Add(time.Minute)
	if o := s.Begin("k", other).Outcome; o != Started {
		t.Fatal("ttl not honoured", o)
	}
	if Started.String() != "started" || InFlight.String() != "in_flight" {
		t.Fatal()
	}
}

func TestAbortAndLease(t *testing.T) {
	now := time.Unix(0, 0)
	s := New(time.Minute, 5*time.Second)
	s.now = func() time.Time { return now }
	fp := FingerprintOf("POST", "/", nil)
	a := s.Begin("k", fp)
	w := s.Begin("k", fp)
	if !s.Abort("k", a.Token) {
		t.Fatal("abort")
	}
	<-w.Ready
	b := s.Begin("k", fp)
	if b.Outcome != Started {
		t.Fatal("abort should release key")
	}
	w = s.Begin("k", fp)
	now = now.Add(6 * time.Second) // lease de b vencido
	// Complete tardío de b después del lease: rechazado.
	if s.Complete("k", b.Token, Response{Status: 201}) {
		t.Fatal("late complete accepted")
	}
	c := s.Begin("k", fp)
	if c.Outcome != Started {
		t.Fatal("lease should expire")
	}
	<-w.Ready
	// b no puede tocar la entrada de c (mismo fingerprint, otro dueño).
	if s.Complete("k", b.Token, Response{Status: 201}) || s.Abort("k", b.Token) {
		t.Fatal("stale owner hijacked key")
	}
	s.Begin("z", fp)
	y := s.Begin("y", fp)
	s.Complete("y", y.Token, Response{})
	now = now.Add(2 * time.Minute)
	if n := s.Sweep(); n != 3 || s.Len() != 0 {
		t.Fatalf("swept %d len %d", n, s.Len())
	}
	if s.Complete("missing", 1, Response{}) || s.Abort("missing", 1) {
		t.Fatal()
	}
}

// Exactamente un ganador por key bajo concurrencia.
func TestConcurrentSingleWinner(t *testing.T) {
	s := New(time.Minute, time.Minute)
	fp := FingerprintOf("POST", "/", []byte("b"))
	var started, replay atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := s.Begin("k", fp)
			switch r.Outcome {
			case Started:
				started.Add(1)
				time.Sleep(5 * time.Millisecond)
				s.Complete("k", r.Token, Response{Status: 201})
			case InFlight:
				<-r.Ready
				if s.Begin("k", fp).Outcome == Replay {
					replay.Add(1)
				}
			case Replay:
				replay.Add(1)
			}
		}()
	}
	wg.Wait()
	if started.Load() != 1 || started.Load()+replay.Load() != 64 {
		t.Fatalf("started=%d replay=%d", started.Load(), replay.Load())
	}
}

// Property-based: comparamos el Store contra un modelo de referencia trivial
// sobre secuencias aleatorias de operaciones (Begin/Complete/Abort/tiempo),
// incluyendo dueños viejos que intentan completar después de perder el lease.
func TestPropertyMatchesModel(t *testing.T) {
	type mstate struct {
		fp      int
		owner   Token
		done    bool
		status  int
		expires int64
	}
	prop := func(seed uint64) bool {
		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		var clock int64
		s := New(30*time.Second, 5*time.Second)
		s.now = func() time.Time { return time.Unix(clock, 0) }
		model := map[string]*mstate{}
		var tokens []struct {
			key string
			tok Token
		}
		fps := []Fingerprint{FingerprintOf("POST", "/", []byte("a")), FingerprintOf("POST", "/", []byte("b"))}
		for step := 0; step < 300; step++ {
			key := fmt.Sprint("k", rng.IntN(3))
			fi := rng.IntN(2)
			m := model[key]
			if m != nil && clock >= m.expires {
				delete(model, key)
				m = nil
			}
			switch rng.IntN(4) {
			case 0: // Begin
				r := s.Begin(key, fps[fi])
				var want Outcome
				switch {
				case m == nil:
					want = Started
					model[key] = &mstate{fp: fi, owner: r.Token, expires: clock + 5}
					tokens = append(tokens, struct {
						key string
						tok Token
					}{key, r.Token})
				case m.fp != fi:
					want = Mismatch
				case !m.done:
					want = InFlight
				default:
					want = Replay
					if r.Response == nil || r.Response.Status != m.status {
						return false
					}
				}
				if r.Outcome != want {
					t.Logf("seed=%d step=%d got %v want %v", seed, step, r.Outcome, want)
					return false
				}
			case 1, 2: // Complete / Abort con un token cualquiera emitido antes
				if len(tokens) == 0 {
					continue
				}
				tk := tokens[rng.IntN(len(tokens))]
				mm := model[tk.key]
				if mm != nil && clock >= mm.expires {
					delete(model, tk.key)
					mm = nil
				}
				valid := mm != nil && mm.owner == tk.tok && !mm.done
				var got bool
				if rng.IntN(2) == 0 {
					st := 200 + rng.IntN(5)
					got = s.Complete(tk.key, tk.tok, Response{Status: st})
					if valid {
						mm.done, mm.status, mm.expires = true, st, clock+30
					}
				} else {
					got = s.Abort(tk.key, tk.tok)
					if valid {
						delete(model, tk.key)
					}
				}
				if got != valid {
					t.Logf("seed=%d step=%d complete/abort got %v want %v", seed, step, got, valid)
					return false
				}
			case 3:
				clock += int64(rng.IntN(8))
			}
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Fatal(err)
	}
}
