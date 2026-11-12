// Package singleflight colapsa llamadas concurrentes con la misma clave en una
// sola ejecución. Es la pieza central contra el "thundering herd" o
// "cache stampede": si 1000 requests piden el mismo ticket justo cuando la
// entrada de caché vence, sólo una va a la base; las otras 999 esperan y
// reciben el mismo resultado.
//
// Se implementa a mano (en vez de golang.org/x/sync/singleflight) para poder
// explicarlo línea por línea en la entrevista y para exponer cuántas llamadas
// fueron compartidas.
package singleflight

import (
	"fmt"
	"sync"
)

type call[V any] struct {
	wg   sync.WaitGroup
	val  V
	err  error
	dups int
}

// Group agrupa llamadas por clave.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// PanicError envuelve un panic ocurrido dentro de fn, para que los que
// esperan no queden bloqueados para siempre.
type PanicError struct{ Value any }

func (p *PanicError) Error() string { return fmt.Sprintf("singleflight: panic in fn: %v", p.Value) }

// Do ejecuta fn una sola vez por clave en vuelo. shared indica si el
// resultado se entregó a más de un llamador.
func (g *Group[K, V]) Do(key K, fn func() (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call[V]{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = &PanicError{Value: r}
			}
		}()
		c.val, c.err = fn()
	}()

	g.mu.Lock()
	delete(g.m, key)
	shared = c.dups > 0
	g.mu.Unlock()
	c.wg.Done()
	return c.val, c.err, shared
}

// InFlight indica si hay una llamada en vuelo para key.
func (g *Group[K, V]) InFlight(key K) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.m[key]
	return ok
}
