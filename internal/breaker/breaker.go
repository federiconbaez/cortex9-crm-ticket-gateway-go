// Package breaker implementa un circuit breaker con ventana deslizante por
// buckets de tiempo.
//
//	closed --(fallas/total >= umbral y total >= mínimo)--> open
//	open   --(pasó OpenTimeout)-----------------------> half_open
//	half_open --(HalfOpenProbes éxitos seguidos)--------> closed
//	half_open --(cualquier falla)-----------------------> open
//
// En open falla rápido (ErrOpen) sin tocar el downstream: protege al
// dependiente de reintentos en cascada y le da tiempo a recuperarse. En
// half_open deja pasar a lo sumo HalfOpenProbes llamadas concurrentes.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State del breaker.
type State int

const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half_open"
	default:
		return "open"
	}
}

// ErrOpen se devuelve cuando el breaker rechaza sin llamar.
var ErrOpen = errors.New("circuit breaker open")

// Config del breaker.
type Config struct {
	Window         time.Duration // ventana de observación (p. ej. 10s)
	Buckets        int           // resolución de la ventana (p. ej. 10)
	MinRequests    int           // mínimo de llamadas para evaluar
	FailureRatio   float64       // p. ej. 0.5
	OpenTimeout    time.Duration // cuánto queda abierto
	HalfOpenProbes int           // éxitos necesarios para cerrar
}

type bucket struct {
	start     time.Time
	ok, fails int
}

// Breaker es seguro para uso concurrente.
type Breaker struct {
	name     string
	cfg      Config
	mu       sync.Mutex
	state    State
	openedAt time.Time
	buckets  []bucket
	probes   int // en vuelo en half-open
	succ     int // éxitos en half-open
	now      func() time.Time
	// OnStateChange se llama (fuera del lock) al cambiar de estado.
	OnStateChange func(name string, from, to State)
}

// New crea un breaker.
func New(name string, cfg Config) *Breaker {
	if cfg.Buckets <= 0 {
		cfg.Buckets = 10
	}
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 1
	}
	return &Breaker{name: name, cfg: cfg, buckets: make([]bucket, cfg.Buckets), now: time.Now}
}

// Name del breaker.
func (b *Breaker) Name() string { return b.name }

// State actual (evalúa el paso open->half_open por tiempo).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		return HalfOpen
	}
	return b.state
}

func (b *Breaker) cur(now time.Time) *bucket {
	width := b.cfg.Window / time.Duration(b.cfg.Buckets)
	idx := int(now.UnixNano()/int64(width)) % b.cfg.Buckets
	start := now.Truncate(width)
	bk := &b.buckets[idx]
	if !bk.start.Equal(start) {
		*bk = bucket{start: start}
	}
	return bk
}

func (b *Breaker) totals(now time.Time) (ok, fails int) {
	for _, bk := range b.buckets {
		if now.Sub(bk.start) < b.cfg.Window {
			ok += bk.ok
			fails += bk.fails
		}
	}
	return
}

func (b *Breaker) setState(to State, now time.Time) func() {
	from := b.state
	if from == to {
		return func() {}
	}
	b.state = to
	b.probes, b.succ = 0, 0
	if to == Open {
		b.openedAt = now
	}
	if to == Closed {
		for i := range b.buckets {
			b.buckets[i] = bucket{}
		}
	}
	cb := b.OnStateChange
	return func() {
		if cb != nil {
			cb(b.name, from, to)
		}
	}
}

// Allow pide permiso para una llamada. Si lo da, el llamador DEBE invocar
// done(success) exactamente una vez.
func (b *Breaker) Allow() (done func(success bool), err error) {
	b.mu.Lock()
	now := b.now()
	notify := func() {}
	if b.state == Open {
		if now.Sub(b.openedAt) < b.cfg.OpenTimeout {
			b.mu.Unlock()
			return nil, ErrOpen
		}
		notify = b.setState(HalfOpen, now)
	}
	if b.state == HalfOpen {
		if b.probes >= b.cfg.HalfOpenProbes {
			b.mu.Unlock()
			notify()
			return nil, ErrOpen
		}
		b.probes++
	}
	b.mu.Unlock()
	notify()
	var once sync.Once
	return func(success bool) { once.Do(func() { b.record(success) }) }, nil
}

func (b *Breaker) record(success bool) {
	b.mu.Lock()
	now := b.now()
	notify := func() {}
	switch b.state {
	case HalfOpen:
		b.probes--
		if !success {
			notify = b.setState(Open, now)
		} else if b.succ++; b.succ >= b.cfg.HalfOpenProbes {
			notify = b.setState(Closed, now)
		}
	case Closed:
		bk := b.cur(now)
		if success {
			bk.ok++
		} else {
			bk.fails++
		}
		ok, fails := b.totals(now)
		total := ok + fails
		if total >= b.cfg.MinRequests && total > 0 && float64(fails)/float64(total) >= b.cfg.FailureRatio {
			notify = b.setState(Open, now)
		}
	case Open:
		// resultado tardío de una llamada iniciada antes de abrir: se ignora.
	}
	b.mu.Unlock()
	notify()
}

// Do es un helper: ejecuta fn bajo el breaker.
func (b *Breaker) Do(fn func() error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	err = fn()
	done(err == nil)
	return err
}
