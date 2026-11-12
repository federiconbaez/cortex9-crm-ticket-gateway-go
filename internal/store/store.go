// Package store implementa un almacén transaccional en memoria con la misma
// semántica que usaríamos sobre Postgres/MySQL: una transacción escribe el
// ticket y el evento de outbox de forma atómica (todo o nada), lecturas con
// paginación keyset y control de concurrencia optimista por versión.
//
// Opcionalmente simula un pool de conexiones de base de datos (N conexiones,
// latencia fija por operación) para poder reproducir saturación real bajo
// carga: cuando el pool se agota, las requests hacen cola, que es exactamente
// lo que pasa con un pool de conexiones en producción.
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrVersionConflict = errors.New("version conflict")
	ErrBadCursor       = errors.New("invalid cursor")
	ErrDuplicate       = errors.New("duplicate id")
)

// OutboxEvent es una fila de la tabla outbox.
type OutboxEvent struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	AggregateID string            `json:"aggregate_id"`
	Priority    ticket.Priority   `json:"priority"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	PublishedAt time.Time         `json:"published_at,omitempty"`
	seq         uint64
}

type row struct {
	t   ticket.Ticket
	seq uint64
}

// Store es el almacén en memoria.
type Store struct {
	mu       sync.RWMutex
	tickets  map[string]*row
	byClient map[string][]*row // ordenado por seq ascendente (append-only)
	outbox   []*OutboxEvent    // pendientes, en orden de inserción
	seq      uint64
	now      func() time.Time

	pool    chan struct{}
	latency time.Duration
}

// Option configura el Store.
type Option func(*Store)

// WithClock inyecta el reloj (tests).
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// WithSimulatedDB simula un pool de conns conexiones con latencia fija por
// operación. conns<=0 desactiva la simulación.
func WithSimulatedDB(conns int, latency time.Duration) Option {
	return func(s *Store) {
		if conns > 0 {
			s.pool = make(chan struct{}, conns)
			s.latency = latency
		}
	}
}

// New crea un Store vacío.
func New(opts ...Option) *Store {
	s := &Store{tickets: map[string]*row{}, byClient: map[string][]*row{}, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// acquire toma una "conexión" del pool simulado respetando el context: si el
// deadline de la request vence esperando, se devuelve el error del context.
func (s *Store) acquire(ctx context.Context) (func(), error) {
	if s.pool == nil {
		return func() {}, ctx.Err()
	}
	select {
	case s.pool <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.latency > 0 {
		t := time.NewTimer(s.latency)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			<-s.pool
			return nil, ctx.Err()
		}
	}
	return func() { <-s.pool }, nil
}

// PoolInUse devuelve cuántas conexiones simuladas están ocupadas.
func (s *Store) PoolInUse() int {
	if s.pool == nil {
		return 0
	}
	return len(s.pool)
}

// Tx es una transacción: acumula escrituras y se aplican juntas en Commit.
type Tx struct {
	s       *Store
	inserts []ticket.Ticket
	updates []update
	events  []OutboxEvent
}

type update struct {
	t        ticket.Ticket
	expected int64
}

// InsertTicket agenda la inserción de un ticket.
func (tx *Tx) InsertTicket(t ticket.Ticket) { tx.inserts = append(tx.inserts, t) }

// UpdateTicket agenda una actualización condicionada a expectedVersion.
func (tx *Tx) UpdateTicket(t ticket.Ticket, expectedVersion int64) {
	tx.updates = append(tx.updates, update{t: t, expected: expectedVersion})
}

// AppendOutbox agenda un evento de outbox en la misma transacción.
func (tx *Tx) AppendOutbox(e OutboxEvent) { tx.events = append(tx.events, e) }

// GetForUpdate lee un ticket dentro de la transacción (ya tenemos el lock).
func (tx *Tx) GetForUpdate(id string) (ticket.Ticket, error) {
	r, ok := tx.s.tickets[id]
	if !ok {
		return ticket.Ticket{}, ErrNotFound
	}
	return r.t, nil
}

// WithTx ejecuta fn dentro de una transacción serializable. Si fn devuelve
// error, nada se aplica (rollback). Si alguna validación de commit falla
// (duplicado, conflicto de versión), tampoco se aplica nada.
func (s *Store) WithTx(ctx context.Context, fn func(*Tx) error) error {
	release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := &Tx{s: s}
	if err := fn(tx); err != nil {
		return err
	}
	// Validación previa: o se aplica todo o nada.
	seen := map[string]bool{}
	for _, t := range tx.inserts {
		if _, ok := s.tickets[t.ID]; ok || seen[t.ID] {
			return fmt.Errorf("%w: %s", ErrDuplicate, t.ID)
		}
		seen[t.ID] = true
	}
	for _, u := range tx.updates {
		r, ok := s.tickets[u.t.ID]
		if !ok {
			return ErrNotFound
		}
		if r.t.Version != u.expected {
			return fmt.Errorf("%w: have %d, expected %d", ErrVersionConflict, r.t.Version, u.expected)
		}
	}
	for _, t := range tx.inserts {
		s.seq++
		r := &row{t: t, seq: s.seq}
		s.tickets[t.ID] = r
		s.byClient[t.ClientID] = append(s.byClient[t.ClientID], r)
	}
	for _, u := range tx.updates {
		s.tickets[u.t.ID].t = u.t
	}
	now := s.now()
	for i := range tx.events {
		e := tx.events[i]
		s.seq++
		e.seq = s.seq
		if e.CreatedAt.IsZero() {
			e.CreatedAt = now
		}
		s.outbox = append(s.outbox, &e)
	}
	return nil
}

// Get devuelve un ticket por id.
func (s *Store) Get(ctx context.Context, id string) (ticket.Ticket, error) {
	release, err := s.acquire(ctx)
	if err != nil {
		return ticket.Ticket{}, err
	}
	defer release()
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.tickets[id]
	if !ok {
		return ticket.Ticket{}, ErrNotFound
	}
	return r.t, nil
}

// ListQuery parametriza List.
type ListQuery struct {
	ClientID string
	Status   ticket.Status
	Limit    int
	Cursor   string
}

// Page es una página de resultados.
type Page struct {
	Items      []ticket.Ticket `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

const cursorPrefix = "v1:"

// EncodeCursor convierte una posición en un cursor opaco.
func EncodeCursor(seq uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatUint(seq, 10)))
}

// DecodeCursor valida y decodifica un cursor.
func DecodeCursor(c string) (uint64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, ErrBadCursor
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil {
		return 0, ErrBadCursor
	}
	return n, nil
}

// List pagina por keyset (seq > cursor) sobre los tickets del cliente. Es
// estable frente a inserciones concurrentes: una fila nueva nunca desplaza
// a las ya vistas, a diferencia de OFFSET.
func (s *Store) List(ctx context.Context, q ListQuery) (Page, error) {
	after, err := DecodeCursor(q.Cursor)
	if err != nil {
		return Page{}, err
	}
	if q.Limit <= 0 || q.Limit > 100 {
		q.Limit = 20
	}
	release, err := s.acquire(ctx)
	if err != nil {
		return Page{}, err
	}
	defer release()
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := s.byClient[q.ClientID]
	i := sort.Search(len(rows), func(i int) bool { return rows[i].seq > after })
	page := Page{Items: make([]ticket.Ticket, 0, q.Limit)}
	var last uint64
	for ; i < len(rows); i++ {
		r := rows[i]
		if q.Status != "" && r.t.Status != q.Status {
			continue
		}
		if len(page.Items) == q.Limit {
			page.NextCursor = EncodeCursor(last)
			break
		}
		page.Items = append(page.Items, r.t)
		last = r.seq
	}
	return page, nil
}

// PendingOutbox devuelve hasta limit eventos pendientes en orden.
func (s *Store) PendingOutbox(limit int) []OutboxEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit > len(s.outbox) {
		limit = len(s.outbox)
	}
	out := make([]OutboxEvent, limit)
	for i := 0; i < limit; i++ {
		out[i] = *s.outbox[i]
	}
	return out
}

// OutboxLen devuelve la cantidad de eventos pendientes.
func (s *Store) OutboxLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.outbox)
}

// OldestPendingAge devuelve la antigüedad del evento pendiente más viejo.
func (s *Store) OldestPendingAge() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.outbox) == 0 {
		return 0
	}
	return s.now().Sub(s.outbox[0].CreatedAt)
}

// MarkPublished saca de pendientes los eventos publicados. Es idempotente.
func (s *Store) MarkPublished(ids []string) int {
	if len(ids) == 0 {
		return 0
	}
	done := make(map[string]bool, len(ids))
	for _, id := range ids {
		done[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.outbox[:0]
	n := 0
	for _, e := range s.outbox {
		if done[e.ID] {
			n++
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(s.outbox); i++ {
		s.outbox[i] = nil
	}
	s.outbox = kept
	return n
}

// Count devuelve el total de tickets.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tickets)
}
