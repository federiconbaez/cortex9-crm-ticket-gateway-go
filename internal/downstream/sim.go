// Package downstream simula dependencias externas (servicio de
// notificaciones, CRM, agente de IA) con latencia y fallas inyectables en
// caliente. Es determinista: usa un PRNG con semilla, así los chaos tests
// son reproducibles.
package downstream

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInjected es la falla inyectada.
var ErrInjected = errors.New("injected downstream failure")

// Chaos es la configuración inyectable.
type Chaos struct {
	Latency   time.Duration `json:"latency"`
	Jitter    time.Duration `json:"jitter"`
	ErrorRate float64       `json:"error_rate"`
}

// Sim es un downstream simulado.
type Sim struct {
	name  string
	mu    sync.Mutex
	chaos Chaos
	rnd   *rand.Rand
	calls atomic.Int64
	fails atomic.Int64
}

// New crea un downstream con semilla fija.
func New(name string, seed uint64, c Chaos) *Sim {
	return &Sim{name: name, chaos: c, rnd: rand.New(rand.NewPCG(seed, seed+1))}
}

// Name del downstream.
func (s *Sim) Name() string { return s.name }

// SetChaos cambia latencia/fallas en caliente.
func (s *Sim) SetChaos(c Chaos) {
	s.mu.Lock()
	s.chaos = c
	s.mu.Unlock()
}

// Chaos devuelve la configuración actual.
func (s *Sim) Chaos() Chaos {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chaos
}

// Calls devuelve llamadas totales y fallidas (inyectadas).
func (s *Sim) Calls() (total, failed int64) { return s.calls.Load(), s.fails.Load() }

// Call simula una llamada: espera la latencia (respetando ctx) y falla con
// probabilidad ErrorRate.
func (s *Sim) Call(ctx context.Context) error {
	s.calls.Add(1)
	s.mu.Lock()
	c := s.chaos
	d := c.Latency
	if c.Jitter > 0 {
		d += time.Duration(s.rnd.Int64N(int64(c.Jitter)))
	}
	fail := c.ErrorRate > 0 && s.rnd.Float64() < c.ErrorRate
	s.mu.Unlock()
	if d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			s.fails.Add(1)
			return ctx.Err()
		}
	} else if err := ctx.Err(); err != nil {
		s.fails.Add(1)
		return err
	}
	if fail {
		s.fails.Add(1)
		return ErrInjected
	}
	return nil
}
