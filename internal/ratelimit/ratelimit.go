// Package ratelimit implementa un token bucket por cliente.
//
// Cada cliente tiene un balde de capacidad Burst que se rellena a Rate
// tokens por segundo. Cada request consume un token; si no hay, se rechaza
// con 429 y Retry-After. El relleno es "perezoso": no hay goroutines por
// cliente, se calcula al momento de cada request a partir del tiempo
// transcurrido. El mapa está shardeado para reducir contención de locks y un
// barrido periódico elimina baldes inactivos (que ya estarían llenos), así la
// memoria queda acotada por clientes activos, no por clientes históricos.
package ratelimit

import (
	"hash/maphash"
	"math"
	"strconv"
	"sync"
	"time"
)

const nShards = 64

type bucket struct {
	tokens float64
	last   time.Time
}

type shard struct {
	mu sync.Mutex
	m  map[string]*bucket
}

// Limiter es un token bucket por clave.
type Limiter struct {
	rate   float64 // tokens/segundo
	burst  float64
	shards [nShards]shard
	seed   maphash.Seed
	now    func() time.Time
}

// Decision es el resultado de Allow.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	Reset      time.Duration // hasta que el balde vuelva a estar lleno
	RetryAfter time.Duration // hasta que haya 1 token (sólo si !Allowed)
}

// New crea un limitador. rate en tokens/s, burst = capacidad.
func New(rate float64, burst int) *Limiter {
	l := &Limiter{rate: rate, burst: float64(burst), seed: maphash.MakeSeed(), now: time.Now}
	for i := range l.shards {
		l.shards[i].m = map[string]*bucket{}
	}
	return l
}

// Allow consume un token de key si hay.
func (l *Limiter) Allow(key string) Decision {
	return l.AllowN(key, 1)
}

// AllowN consume n tokens de key si hay.
func (l *Limiter) AllowN(key string, n int) Decision {
	now := l.now()
	sh := &l.shards[maphash.String(l.seed, key)%nShards]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b, ok := sh.m[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		sh.m[key] = b
	}
	if el := now.Sub(b.last); el > 0 {
		b.tokens = math.Min(l.burst, b.tokens+el.Seconds()*l.rate)
		b.last = now
	}
	d := Decision{Limit: int(l.burst)}
	need := float64(n)
	if b.tokens >= need {
		b.tokens -= need
		d.Allowed = true
	} else {
		d.RetryAfter = l.secs(need - b.tokens)
	}
	d.Remaining = int(math.Floor(b.tokens))
	d.Reset = l.secs(l.burst - b.tokens)
	return d
}

func (l *Limiter) secs(tokens float64) time.Duration {
	if l.rate <= 0 {
		return time.Hour
	}
	return time.Duration(tokens / l.rate * float64(time.Second))
}

// Sweep borra baldes que estarían llenos (inactivos). Devuelve cuántos.
func (l *Limiter) Sweep() int {
	now := l.now()
	full := l.secs(l.burst)
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for k, b := range sh.m {
			if now.Sub(b.last) >= full {
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// Len devuelve cuántos baldes hay en memoria.
func (l *Limiter) Len() int {
	n := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].m)
		l.shards[i].mu.Unlock()
	}
	return n
}

// Policy devuelve el valor del header RateLimit-Policy (draft IETF):
// "<burst>;w=<segundos para rellenar>".
func (l *Limiter) Policy() string {
	w := int(math.Ceil(l.secs(l.burst).Seconds()))
	return strconv.Itoa(int(l.burst)) + ";w=" + strconv.Itoa(w)
}

// CeilSeconds redondea hacia arriba a segundos enteros (mínimo 1 si d>0),
// formato que exigen Retry-After y RateLimit-Reset.
func CeilSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}
