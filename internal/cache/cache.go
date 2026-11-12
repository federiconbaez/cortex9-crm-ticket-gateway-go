// Package cache implementa una caché de lectura con TTL, protección
// anti-stampede (singleflight) y expiración temprana probabilística (XFetch,
// Vattani, Chierichetti y Lowenstein, VLDB 2015).
//
// XFetch: cada lectura decide recomputar antes de que venza si
//
//	now - delta * beta * ln(rand()) >= expiry
//
// donde delta es lo que tardó la última recomputación y beta (>0, típico 1)
// controla la agresividad. Como -ln(rand) es exponencial, la probabilidad de
// refrescar crece a medida que nos acercamos al vencimiento y es mayor para
// valores caros (delta grande). En la práctica casi nunca hay un instante en
// que la entrada esté vencida y todos la pidan a la vez.
//
// Decisión: el refresh temprano se hace en segundo plano (el llamador recibe
// el valor vigente, que todavía no venció), y se deduplica con singleflight.
// Si la entrada ya venció, el llamador espera la carga (también colapsada).
package cache

import (
	"context"
	"hash/maphash"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/singleflight"
)

// Result clasifica el resultado de una lectura (para métricas).
type Result string

const (
	Hit          Result = "hit"
	Miss         Result = "miss"
	Shared       Result = "shared"        // esperó una carga ya en vuelo
	EarlyRefresh Result = "early_refresh" // hit + refresh XFetch disparado
)

type entry[V any] struct {
	val    V
	delta  time.Duration
	expiry time.Time
}

const shards = 32

type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]*entry[V]
}

// Config de la caché.
type Config struct {
	TTL         time.Duration
	Beta        float64 // 0 desactiva XFetch
	MaxEntries  int     // por shard*shards aproximado; 0 = 10000
	LoadTimeout time.Duration
}

// Cache es una caché genérica read-through.
type Cache[K comparable, V any] struct {
	cfg    Config
	shards [shards]shard[K, V]
	seed   maphash.Seed
	sf     singleflight.Group[K, V]
	now    func() time.Time
	rnd    func() float64
	hash   func(K) uint64
	// OnResult se llama con cada resultado (métricas). Puede ser nil.
	OnResult func(Result)
	wg       sync.WaitGroup
}

// New crea una caché. hash debe ser estable para K.
func New[K comparable, V any](cfg Config, hash func(maphash.Seed, K) uint64) *Cache[K, V] {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 10000
	}
	if cfg.LoadTimeout <= 0 {
		cfg.LoadTimeout = time.Second
	}
	c := &Cache[K, V]{cfg: cfg, seed: maphash.MakeSeed(), now: time.Now, rnd: rand.Float64}
	c.hash = func(k K) uint64 { return hash(c.seed, k) }
	for i := range c.shards {
		c.shards[i].m = make(map[K]*entry[V])
	}
	return c
}

// StringHash es un hash para claves string.
func StringHash(seed maphash.Seed, k string) uint64 { return maphash.String(seed, k) }

func (c *Cache[K, V]) shard(k K) *shard[K, V] { return &c.shards[c.hash(k)%shards] }

func (c *Cache[K, V]) report(r Result) {
	if c.OnResult != nil {
		c.OnResult(r)
	}
}

// shouldEarlyRefresh implementa la condición XFetch.
func (c *Cache[K, V]) shouldEarlyRefresh(e *entry[V], now time.Time) bool {
	if c.cfg.Beta <= 0 || e.delta <= 0 {
		return false
	}
	r := c.rnd()
	if r <= 0 {
		r = math.SmallestNonzeroFloat64
	}
	gap := time.Duration(float64(e.delta) * c.cfg.Beta * -math.Log(r))
	return !now.Add(gap).Before(e.expiry)
}

// Get devuelve el valor de k, cargándolo con load si hace falta. El context
// del llamador acota cuánto espera él; la carga compartida usa un context
// propio con LoadTimeout, para que un cliente que cancela no aborte la carga
// que están esperando los demás.
func (c *Cache[K, V]) Get(ctx context.Context, k K, load func(context.Context) (V, error)) (V, Result, error) {
	now := c.now()
	sh := c.shard(k)
	sh.mu.RLock()
	e, ok := sh.m[k]
	sh.mu.RUnlock()
	if ok && now.Before(e.expiry) {
		if c.shouldEarlyRefresh(e, now) && !c.sf.InFlight(k) {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				_, _, _ = c.sf.Do(k, func() (V, error) { return c.loadAndStore(k, load) })
			}()
			c.report(EarlyRefresh)
			return e.val, EarlyRefresh, nil
		}
		c.report(Hit)
		return e.val, Hit, nil
	}
	type res struct {
		v      V
		err    error
		shared bool
	}
	ch := make(chan res, 1)
	go func() {
		v, err, shared := c.sf.Do(k, func() (V, error) { return c.loadAndStore(k, load) })
		ch <- res{v, err, shared}
	}()
	select {
	case r := <-ch:
		result := Miss
		if r.shared {
			result = Shared
		}
		c.report(result)
		return r.v, result, r.err
	case <-ctx.Done():
		var zero V
		return zero, Miss, ctx.Err()
	}
}

func (c *Cache[K, V]) loadAndStore(k K, load func(context.Context) (V, error)) (V, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.LoadTimeout)
	defer cancel()
	start := c.now()
	v, err := load(ctx)
	if err != nil {
		return v, err
	}
	end := c.now()
	c.Set(k, v, end.Sub(start))
	return v, nil
}

// Set guarda v con el TTL configurado; delta es el costo de recomputarlo.
func (c *Cache[K, V]) Set(k K, v V, delta time.Duration) {
	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, exists := sh.m[k]; !exists && len(sh.m) >= c.cfg.MaxEntries/shards+1 {
		for victim := range sh.m { // desalojo aleatorio O(1)
			delete(sh.m, victim)
			break
		}
	}
	sh.m[k] = &entry[V]{val: v, delta: delta, expiry: c.now().Add(c.cfg.TTL)}
}

// Invalidate borra k (escrituras hacen invalidate-on-write).
func (c *Cache[K, V]) Invalidate(k K) {
	sh := c.shard(k)
	sh.mu.Lock()
	delete(sh.m, k)
	sh.mu.Unlock()
}

// Len devuelve la cantidad de entradas.
func (c *Cache[K, V]) Len() int {
	n := 0
	for i := range c.shards {
		c.shards[i].mu.RLock()
		n += len(c.shards[i].m)
		c.shards[i].mu.RUnlock()
	}
	return n
}

// Wait espera refreshes en segundo plano (shutdown y tests).
func (c *Cache[K, V]) Wait() { c.wg.Wait() }
