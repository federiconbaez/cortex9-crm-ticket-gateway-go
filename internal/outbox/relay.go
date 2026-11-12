// Package outbox implementa el relay del patrón transactional outbox.
//
// El problema: "guardar el ticket y publicar el evento" son dos sistemas
// distintos (base y cola). Si hacemos los dos por separado, un crash en el
// medio deja un ticket sin evento (o un evento sin ticket). La solución:
// escribir el evento en una tabla outbox DENTRO de la misma transacción que
// el ticket, y que un proceso aparte (este relay) lea la outbox y publique.
// La entrega resultante es at-least-once: si el relay publica y muere antes
// de marcar, el evento se publica de nuevo; por eso el consumidor deduplica
// por ID de evento.
package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/queue"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/store"
)

// Source es lo que el relay necesita del store.
type Source interface {
	PendingOutbox(limit int) []store.OutboxEvent
	MarkPublished(ids []string) int
	OutboxLen() int
}

// Publisher publica un evento. queue.ErrDuplicate cuenta como publicado.
type Publisher func(store.OutboxEvent) error

// Relay mueve eventos de la outbox a la cola.
type Relay struct {
	Src      Source
	Publish  Publisher
	Interval time.Duration
	Batch    int
	kick     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	// OnFlush (opcional): publicados y errores por flush.
	OnFlush func(published, failed int)
}

// NewRelay crea un relay.
func NewRelay(src Source, pub Publisher, interval time.Duration, batch int) *Relay {
	if batch <= 0 {
		batch = 256
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	return &Relay{Src: src, Publish: pub, Interval: interval, Batch: batch, kick: make(chan struct{}, 1), done: make(chan struct{})}
}

// Kick despierta al relay sin esperar al próximo tick (baja latencia
// end-to-end sin hacer polling agresivo).
func (r *Relay) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Flush publica un lote. Devuelve publicados y si quedó trabado (cola llena
// u otro error) para que el loop haga backoff.
func (r *Relay) Flush() (published int, blocked bool) {
	evs := r.Src.PendingOutbox(r.Batch)
	ids := make([]string, 0, len(evs))
	failed := 0
	for _, e := range evs {
		err := r.Publish(e)
		if err == nil || errors.Is(err, queue.ErrDuplicate) {
			ids = append(ids, e.ID)
			continue
		}
		failed++
		blocked = true
		break // preservamos el orden: no salteamos eventos
	}
	published = r.Src.MarkPublished(ids)
	if r.OnFlush != nil && (published > 0 || failed > 0) {
		r.OnFlush(published, failed)
	}
	return published, blocked
}

// Start lanza el loop.
func (r *Relay) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() {
		defer close(r.done)
		t := time.NewTicker(r.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-r.kick:
			}
			for {
				n, blocked := r.Flush()
				if blocked || n < r.Batch {
					break
				}
			}
		}
	}()
}

// Stop detiene el loop y hace un drenaje final hasta vaciar la outbox o
// hasta que venza ctx. Devuelve cuántos eventos quedaron pendientes.
func (r *Relay) Stop(ctx context.Context) int {
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
	for r.Src.OutboxLen() > 0 {
		if ctx.Err() != nil {
			break
		}
		if _, blocked := r.Flush(); blocked {
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	return r.Src.OutboxLen()
}
