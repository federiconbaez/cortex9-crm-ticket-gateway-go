// Package limiter implementa load shedding adaptativo con un límite de
// concurrencia AIMD (additive increase, multiplicative decrease), la misma
// idea que el control de congestión de TCP aplicada a requests en vuelo.
//
// Por qué concurrencia y no RPS: por la ley de Little, en vuelo = RPS x
// latencia. Cuando una dependencia (la base) se satura, la latencia sube y
// con RPS constante crece la concurrencia; si no la acotamos, las requests
// hacen cola en el pool y TODAS terminan lentas o con timeout (colapso por
// congestión). Acotando la concurrencia, el exceso se rechaza rápido (503 +
// Retry-After) y las aceptadas mantienen latencia sana.
//
// Regla:
//   - cada request que termina con latencia <= Target y sin error de
//     sobrecarga: limit += 1/limit (≈ +1 por "ventana" de limit requests),
//     sólo si el límite se estaba usando (inflight >= limit/2);
//   - si termina lenta o con timeout: limit *= Backoff (p. ej. 0.9), como
//     mucho una vez por Cooldown para no desplomarse por una ráfaga.
package limiter

import (
	"math"
	"sync"
	"time"
)

// Config del limitador.
type Config struct {
	Initial  int
	Min      int
	Max      int
	Target   time.Duration // latencia por encima de la cual se considera congestión
	Backoff  float64       // factor multiplicativo (0,1)
	Cooldown time.Duration // mínimo entre dos decrementos
}

// AIMD es seguro para uso concurrente.
type AIMD struct {
	cfg      Config
	mu       sync.Mutex
	limit    float64
	inflight int
	lastDec  time.Time
	now      func() time.Time
}

// New crea un limitador AIMD.
func New(cfg Config) *AIMD {
	if cfg.Min <= 0 {
		cfg.Min = 1
	}
	if cfg.Max < cfg.Min {
		cfg.Max = cfg.Min
	}
	if cfg.Initial < cfg.Min || cfg.Initial > cfg.Max {
		cfg.Initial = cfg.Min
	}
	if cfg.Backoff <= 0 || cfg.Backoff >= 1 {
		cfg.Backoff = 0.9
	}
	return &AIMD{cfg: cfg, limit: float64(cfg.Initial), now: time.Now}
}

// Outcome de una request para el limitador.
type Outcome int

const (
	Success Outcome = iota // latencia medida es válida
	Dropped                // timeout / sobrecarga: señal de congestión
	Ignore                 // error del cliente (4xx): no aporta señal
)

// Acquire pide un lugar. Si ok, hay que llamar release exactamente una vez.
func (l *AIMD) Acquire() (release func(Outcome), ok bool) {
	l.mu.Lock()
	if l.inflight >= int(l.limit) {
		l.mu.Unlock()
		return nil, false
	}
	l.inflight++
	l.mu.Unlock()
	start := l.now()
	var once sync.Once
	return func(o Outcome) {
		once.Do(func() { l.onDone(o, l.now().Sub(start)) })
	}, true
}

func (l *AIMD) onDone(o Outcome, rtt time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	inflightAtStart := l.inflight
	l.inflight--
	if o == Ignore {
		return
	}
	now := l.now()
	if o == Dropped || rtt > l.cfg.Target {
		if now.Sub(l.lastDec) >= l.cfg.Cooldown {
			l.limit = math.Max(float64(l.cfg.Min), math.Floor(l.limit*l.cfg.Backoff))
			l.lastDec = now
		}
		return
	}
	if float64(inflightAtStart) >= l.limit/2 {
		l.limit = math.Min(float64(l.cfg.Max), l.limit+1/l.limit)
	}
}

// Limit devuelve el límite actual (entero efectivo).
func (l *AIMD) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.limit)
}

// InFlight devuelve las requests en vuelo.
func (l *AIMD) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight
}
