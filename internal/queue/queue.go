// Package queue es una cola de trabajo en memoria con:
//   - prioridad por SLA (P1 antes que P4) con anti-starvation (1 de cada
//     FairEvery pops toma el mensaje más viejo sin importar la prioridad);
//   - reintentos con backoff exponencial y "full jitter";
//   - DLQ (dead letter queue) tras MaxAttempts o error permanente, con redrive;
//   - deduplicación: un ID que ya está en cola o se procesó hace poco no se
//     vuelve a encolar (consumidor idempotente frente a la entrega
//     at-least-once del outbox);
//   - backpressure: capacidad acotada, Enqueue devuelve ErrFull;
//   - shutdown con drenaje acotado por deadline.
//
// En producción esto sería una cola gestionada y durable (SQS, Kafka,
// Pub/Sub o la cola interna de la empresa); la interfaz y la semántica son
// las mismas.
package queue

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrFull      = errors.New("queue full")
	ErrClosed    = errors.New("queue closed")
	ErrDuplicate = errors.New("duplicate message")
)

// permanent marca errores que no tiene sentido reintentar.
type permanent struct{ err error }

func (p permanent) Error() string { return "permanent: " + p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent envuelve err para mandarlo directo a la DLQ.
func Permanent(err error) error { return permanent{err} }

// IsPermanent indica si err es permanente.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Message es una unidad de trabajo.
type Message struct {
	ID         string
	Type       string
	Rank       int // 0 = más urgente
	Payload    []byte
	Headers    map[string]string
	Attempt    int // intentos ya hechos
	EnqueuedAt time.Time
	LastError  string

	seq       uint64
	available time.Time
	index     int
}

// DeadLetter es un mensaje en la DLQ.
type DeadLetter struct {
	Message Message   `json:"message"`
	Reason  string    `json:"reason"`
	At      time.Time `json:"at"`
}

// Handler procesa un mensaje.
type Handler func(ctx context.Context, m Message) error

// Config de la cola.
type Config struct {
	Workers        int
	Capacity       int
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	HandlerTimeout time.Duration
	DedupeTTL      time.Duration
	FairEvery      int
}

// Event para métricas.
type Event string

const (
	EvProcessed Event = "processed"
	EvRetry     Event = "retry"
	EvDLQ       Event = "dlq"
	EvDuplicate Event = "duplicate"
)

// Queue es la cola.
type Queue struct {
	cfg       Config
	mu        sync.Mutex
	ready     readyHeap
	delayed   delayHeap
	ids       map[string]bool
	processed map[string]time.Time
	dlq       []DeadLetter
	seq       uint64
	pops      int
	inflight  int
	closed    bool
	draining  bool
	wake      chan struct{}
	stop      chan struct{}
	hardCtx   context.Context
	hardStop  context.CancelFunc
	wg        sync.WaitGroup
	now       func() time.Time
	rndMu     sync.Mutex
	rnd       *rand.Rand

	// OnEvent (opcional) para métricas; se llama fuera del lock.
	OnEvent func(e Event, m Message, took time.Duration)
}

// New crea la cola.
func New(cfg Config) *Queue {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 10000
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 5 * time.Second
	}
	if cfg.DedupeTTL <= 0 {
		cfg.DedupeTTL = 10 * time.Minute
	}
	if cfg.FairEvery <= 0 {
		cfg.FairEvery = 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		cfg: cfg, ids: map[string]bool{}, processed: map[string]time.Time{},
		wake: make(chan struct{}, cfg.Workers), stop: make(chan struct{}),
		hardCtx: ctx, hardStop: cancel, now: time.Now,
		rnd: rand.New(rand.NewPCG(1, 2)),
	}
}

// SetSeed fija la semilla del jitter (tests reproducibles).
func (q *Queue) SetSeed(a, b uint64) {
	q.rndMu.Lock()
	q.rnd = rand.New(rand.NewPCG(a, b))
	q.rndMu.Unlock()
}

// Backoff devuelve el delay para el intento attempt (1-based) con full
// jitter: uniforme en [0, min(Max, Base*2^(attempt-1))).
func (q *Queue) Backoff(attempt int) time.Duration {
	ceil := float64(q.cfg.BaseBackoff) * math.Pow(2, float64(attempt-1))
	if ceil > float64(q.cfg.MaxBackoff) || math.IsInf(ceil, 0) {
		ceil = float64(q.cfg.MaxBackoff)
	}
	q.rndMu.Lock()
	f := q.rnd.Float64()
	q.rndMu.Unlock()
	return time.Duration(f * ceil)
}

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) emit(e Event, m Message, took time.Duration) {
	if q.OnEvent != nil {
		q.OnEvent(e, m, took)
	}
}

// Enqueue agrega m. Devuelve ErrDuplicate si m.ID ya está en cola o se
// procesó dentro de DedupeTTL (el llamador puede tratarlo como éxito).
func (q *Queue) Enqueue(m Message) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	now := q.now()
	if q.ids[m.ID] {
		q.mu.Unlock()
		q.emit(EvDuplicate, m, 0)
		return ErrDuplicate
	}
	if t, ok := q.processed[m.ID]; ok && now.Sub(t) < q.cfg.DedupeTTL {
		q.mu.Unlock()
		q.emit(EvDuplicate, m, 0)
		return ErrDuplicate
	}
	if q.ready.Len()+q.delayed.Len()+q.inflight >= q.cfg.Capacity {
		q.mu.Unlock()
		return ErrFull
	}
	q.seq++
	m.seq = q.seq
	if m.EnqueuedAt.IsZero() {
		m.EnqueuedAt = now
	}
	m.available = now
	q.ids[m.ID] = true
	mm := m
	heap.Push(&q.ready, &mm)
	q.mu.Unlock()
	q.signal()
	return nil
}

// Start lanza los workers.
func (q *Queue) Start(h Handler) {
	for i := 0; i < q.cfg.Workers; i++ {
		q.wg.Add(1)
		go q.worker(h)
	}
}

func (q *Queue) promote(now time.Time) {
	for q.delayed.Len() > 0 && !q.delayed[0].available.After(now) {
		m := heap.Pop(&q.delayed).(*Message)
		heap.Push(&q.ready, m)
	}
}

// next devuelve el próximo mensaje o nil si hay que salir.
func (q *Queue) next() *Message {
	for {
		q.mu.Lock()
		now := q.now()
		q.promote(now)
		if q.ready.Len() > 0 {
			q.pops++
			var m *Message
			if q.pops%q.cfg.FairEvery == 0 {
				oldest := 0
				for i, c := range q.ready {
					if c.seq < q.ready[oldest].seq {
						oldest = i
					}
				}
				m = heap.Remove(&q.ready, oldest).(*Message)
			} else {
				m = heap.Pop(&q.ready).(*Message)
			}
			q.inflight++
			q.mu.Unlock()
			return m
		}
		if q.draining && q.delayed.Len() == 0 {
			q.mu.Unlock()
			return nil
		}
		wait := time.Hour
		if q.delayed.Len() > 0 {
			wait = q.delayed[0].available.Sub(now)
		}
		q.mu.Unlock()
		t := time.NewTimer(wait)
		select {
		case <-q.wake:
		case <-t.C:
		case <-q.stop:
			t.Stop()
			q.mu.Lock()
			drain := q.draining
			q.mu.Unlock()
			if !drain {
				return nil
			}
			// en drenaje seguimos hasta vaciar; esperamos un poco para no girar
			select {
			case <-q.wake:
			case <-time.After(minDur(wait, 5*time.Millisecond)):
			case <-q.hardCtx.Done():
				return nil
			}
		case <-q.hardCtx.Done():
			t.Stop()
			return nil
		}
		t.Stop()
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (q *Queue) worker(h Handler) {
	defer q.wg.Done()
	for {
		m := q.next()
		if m == nil {
			return
		}
		q.process(h, m)
	}
}

func (q *Queue) process(h Handler, m *Message) {
	ctx, cancel := context.WithTimeout(q.hardCtx, q.cfg.HandlerTimeout)
	start := q.now()
	err := safeCall(ctx, h, *m)
	cancel()
	took := q.now().Sub(start)
	m.Attempt++
	q.mu.Lock()
	q.inflight--
	now := q.now()
	if err == nil {
		delete(q.ids, m.ID)
		q.processed[m.ID] = now
		q.mu.Unlock()
		q.emit(EvProcessed, *m, took)
		return
	}
	m.LastError = err.Error()
	if IsPermanent(err) || m.Attempt >= q.cfg.MaxAttempts {
		reason := "max_attempts"
		if IsPermanent(err) {
			reason = "permanent"
		}
		delete(q.ids, m.ID)
		q.dlq = append(q.dlq, DeadLetter{Message: *m, Reason: reason, At: now})
		q.mu.Unlock()
		q.emit(EvDLQ, *m, took)
		return
	}
	m.available = now.Add(q.Backoff(m.Attempt))
	// Copia tomada bajo el lock: apenas el mensaje vuelve al heap, otro
	// worker puede promoverlo y modificarlo (index, Attempt). Leer *m
	// después de Unlock era una carrera real detectada con -race.
	snap := *m
	heap.Push(&q.delayed, m)
	q.mu.Unlock()
	q.signal()
	q.emit(EvRetry, snap, took)
}

func safeCall(ctx context.Context, h Handler, m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = Permanent(errors.New("handler panic"))
		}
	}()
	return h(ctx, m)
}

// Stats es una foto del estado.
type Stats struct {
	Ready, Delayed, InFlight, DLQ int
}

// Stats devuelve el estado actual.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{Ready: q.ready.Len(), Delayed: q.delayed.Len(), InFlight: q.inflight, DLQ: len(q.dlq)}
}

// DLQ devuelve una copia de la DLQ.
func (q *Queue) DLQ() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]DeadLetter(nil), q.dlq...)
}

// Redrive reencola todos los mensajes de la DLQ con contador en cero.
func (q *Queue) Redrive() int {
	q.mu.Lock()
	dl := q.dlq
	q.dlq = nil
	q.mu.Unlock()
	n := 0
	for _, d := range dl {
		m := d.Message
		m.Attempt, m.LastError, m.EnqueuedAt = 0, "", time.Time{}
		if err := q.Enqueue(m); err == nil {
			n++
		} else {
			q.mu.Lock()
			q.dlq = append(q.dlq, d)
			q.mu.Unlock()
		}
	}
	return n
}

// SweepProcessed limpia el set de deduplicación.
func (q *Queue) SweepProcessed() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	n := 0
	for id, t := range q.processed {
		if now.Sub(t) >= q.cfg.DedupeTTL {
			delete(q.processed, id)
			n++
		}
	}
	return n
}

// Shutdown deja de aceptar mensajes y drena (incluidos reintentos
// programados) hasta que ctx venza; entonces corta los handlers en vuelo.
// Devuelve cuántos mensajes quedaron sin procesar.
func (q *Queue) Shutdown(ctx context.Context) int {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.draining = true
		close(q.stop)
	}
	q.mu.Unlock()
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		q.hardStop()
		<-done
	}
	q.hardStop()
	s := q.Stats()
	return s.Ready + s.Delayed + s.InFlight
}

type readyHeap []*Message

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	if h[i].Rank != h[j].Rank {
		return h[i].Rank < h[j].Rank
	}
	return h[i].seq < h[j].seq
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *readyHeap) Push(x any)   { m := x.(*Message); m.index = len(*h); *h = append(*h, m) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return m
}

type delayHeap []*Message

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].available.Before(h[j].available) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(*Message)) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	m := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return m
}
