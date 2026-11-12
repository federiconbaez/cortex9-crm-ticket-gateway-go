// Package api expone la API REST de tickets y su cadena de middlewares.
//
// Cadena por request (de afuera hacia adentro):
//
//	observe (RED + span + recover) -> mux -> withClient -> rate limit
//	  -> load shedding -> timeout -> idempotency -> handler
//
// El orden importa: lo barato y que más rechaza va primero (rate limit antes
// que shedding), el timeout envuelve sólo el trabajo útil, y la idempotencia
// queda lo más cerca posible del efecto para no memorizar un 429/503.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/cache"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/idempotency"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/limiter"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/observability"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ratelimit"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/store"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/triage"
)

// Config de la capa HTTP.
type Config struct {
	RequestTimeout        time.Duration
	MaxBodyBytes          int64
	IdempotencyWait       time.Duration // 0 => 409 inmediato si está en vuelo
	RequireIdempotencyKey bool
	SlowLog               time.Duration
}

// Deps son las dependencias del server.
type Deps struct {
	Store     *store.Store
	Cache     *cache.Cache[string, ticket.Ticket]
	Idem      *idempotency.Store
	RateLimit *ratelimit.Limiter
	Shedder   *limiter.AIMD
	Metrics   *observability.Metrics
	Tracer    trace.Tracer
	Log       *slog.Logger
	OnCommit  func() // despierta al relay de outbox
	Now       func() time.Time
}

// Server es el handler HTTP.
type Server struct {
	cfg    Config
	st     *store.Store
	cache  *cache.Cache[string, ticket.Ticket]
	idem   *idempotency.Store
	m      *observability.Metrics
	tracer trace.Tracer
	log    *slog.Logger
	kick   func()
	now    func() time.Time
	h      http.Handler
	ready  func() bool
}

// New construye el server y su router.
func New(cfg Config, d Deps, ready func() bool) *Server {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 64 << 10
	}
	if cfg.SlowLog <= 0 {
		cfg.SlowLog = time.Second
	}
	if d.Log == nil {
		d.Log = discardLogger()
	}
	if d.OnCommit == nil {
		d.OnCommit = func() {}
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	s := &Server{cfg: cfg, st: d.Store, cache: d.Cache, idem: d.Idem, m: d.Metrics, tracer: d.Tracer, log: d.Log, kick: d.OnCommit, now: d.Now, ready: ready}
	mux := http.NewServeMux()
	chain := func(h http.HandlerFunc) http.Handler {
		var x http.Handler = h
		x = s.withIdempotency(x)
		x = withTimeout(cfg.RequestTimeout, x)
		x = s.withShedding(d.Shedder, x)
		x = s.withRateLimit(d.RateLimit, x)
		x = withClient(x)
		return x
	}
	mux.Handle("POST /v1/tickets", chain(s.createTicket))
	mux.Handle("GET /v1/tickets", chain(s.listTickets))
	mux.Handle("GET /v1/tickets/{id}", chain(s.getTicket))
	mux.Handle("PATCH /v1/tickets/{id}/status", chain(s.patchStatus))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.ready != nil && !s.ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	s.h = s.observe(mux)
	return s
}

// ServeHTTP implementa http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "tkt_" + hex.EncodeToString(b[:])
}

func etag(v int64) string { return `"v` + strconv.FormatInt(v, 10) + `"` }

func (s *Server) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "not-found", "ticket not found")
	case errors.Is(err, store.ErrBadCursor):
		writeProblem(w, http.StatusBadRequest, "bad-cursor", "invalid pagination cursor")
	case errors.Is(err, store.ErrVersionConflict):
		writeProblem(w, http.StatusPreconditionFailed, "version-conflict", err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, "timeout", "request deadline exceeded")
	default:
		s.log.Error("store", "err", err)
		writeProblem(w, http.StatusInternalServerError, "internal", "unexpected error")
	}
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeProblem(w, http.StatusRequestEntityTooLarge, "body-too-large", err.Error())
		} else {
			writeProblem(w, http.StatusBadRequest, "bad-json", err.Error())
		}
		return false
	}
	return true
}

type createResp struct {
	ticket.Ticket
}

func (s *Server) createTicket(w http.ResponseWriter, r *http.Request) {
	var in ticket.CreateInput
	if !s.decode(w, r, &in) {
		return
	}
	if err := in.Validate(); err != nil {
		writeProblem(w, http.StatusUnprocessableEntity, "validation", err.Error())
		return
	}
	now := s.now().UTC()
	t := ticket.Ticket{
		ID: newID(), ClientID: clientFrom(r.Context()), CustomerID: in.CustomerID, Subject: in.Subject, Body: in.Body,
		Channel: in.Channel, Status: ticket.StatusOpen, Priority: in.Priority, PrioritySource: "client", Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if t.Priority == "" {
		t.Priority = triage.Rules(in.Subject, in.Body).Priority
		t.PrioritySource = "rules"
	}
	t.DueAt = now.Add(t.Priority.SLA())
	payload, _ := json.Marshal(map[string]string{"ticket_id": t.ID, "client_id": t.ClientID})
	err := s.st.WithTx(r.Context(), func(tx *store.Tx) error {
		tx.InsertTicket(t)
		tx.AppendOutbox(store.OutboxEvent{ID: "evt_" + t.ID + "_created", Type: "ticket.created", AggregateID: t.ID,
			Priority: t.Priority, Payload: payload, Headers: observability.Inject(r.Context())})
		return nil
	})
	if err != nil {
		s.storeError(w, err)
		return
	}
	s.kick()
	w.Header().Set("Location", "/v1/tickets/"+t.ID)
	w.Header().Set("ETag", etag(t.Version))
	writeJSON(w, http.StatusCreated, createResp{t})
}

func (s *Server) getTicket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, res, err := s.cache.Get(r.Context(), id, func(ctx context.Context) (ticket.Ticket, error) {
		return s.st.Get(ctx, id)
	})
	s.m.Cache.WithLabelValues(string(res)).Inc()
	if err != nil {
		s.storeError(w, err)
		return
	}
	if t.ClientID != clientFrom(r.Context()) {
		// 404 y no 403: no revelamos que el ticket existe para otro tenant.
		writeProblem(w, http.StatusNotFound, "not-found", "ticket not found")
		return
	}
	tag := etag(t.Version)
	w.Header().Set("ETag", tag)
	w.Header().Set("X-Cache", string(res))
	if r.Header.Get("If-None-Match") == tag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) listTickets(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeProblem(w, http.StatusBadRequest, "bad-limit", "limit must be 1..100")
			return
		}
		limit = n
	}
	st := ticket.Status(q.Get("status"))
	if st != "" && !ticket.ValidStatus(st) {
		writeProblem(w, http.StatusBadRequest, "bad-status", "unknown status filter")
		return
	}
	page, err := s.st.List(r.Context(), store.ListQuery{ClientID: clientFrom(r.Context()), Status: st, Limit: limit, Cursor: q.Get("cursor")})
	if err != nil {
		s.storeError(w, err)
		return
	}
	if page.NextCursor != "" {
		next := fmt.Sprintf("/v1/tickets?limit=%d&cursor=%s", limit, page.NextCursor)
		if st != "" {
			next += "&status=" + string(st)
		}
		w.Header().Set("Link", "<"+next+`>; rel="next"`)
	}
	writeJSON(w, http.StatusOK, page)
}

type statusReq struct {
	Status ticket.Status `json:"status"`
}

func (s *Server) patchStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var in statusReq
	if !s.decode(w, r, &in) {
		return
	}
	if !ticket.ValidStatus(in.Status) {
		writeProblem(w, http.StatusUnprocessableEntity, "validation", "unknown status")
		return
	}
	var expected int64 = -1
	if im := r.Header.Get("If-Match"); im != "" {
		v, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(im, `"v`), `"`), 10, 64)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "bad-if-match", `If-Match must look like "v<version>"`)
			return
		}
		expected = v
	}
	client := clientFrom(r.Context())
	var out ticket.Ticket
	var from ticket.Status
	errNotOwner := errors.New("not owner")
	err := s.st.WithTx(r.Context(), func(tx *store.Tx) error {
		cur, err := tx.GetForUpdate(id)
		if err != nil {
			return err
		}
		if cur.ClientID != client {
			return errNotOwner
		}
		if expected >= 0 && cur.Version != expected {
			return fmt.Errorf("%w: have %d, If-Match %d", store.ErrVersionConflict, cur.Version, expected)
		}
		from = cur.Status
		ver := cur.Version
		if err := cur.Transition(in.Status, s.now().UTC()); err != nil {
			return err
		}
		tx.UpdateTicket(cur, ver)
		payload, _ := json.Marshal(map[string]string{"ticket_id": id, "from": string(from), "to": string(in.Status)})
		tx.AppendOutbox(store.OutboxEvent{ID: fmt.Sprintf("evt_%s_v%d", id, cur.Version), Type: "ticket.status_changed",
			AggregateID: id, Priority: cur.Priority, Payload: payload, Headers: observability.Inject(r.Context())})
		out = cur
		return nil
	})
	switch {
	case errors.Is(err, errNotOwner):
		writeProblem(w, http.StatusNotFound, "not-found", "ticket not found")
		return
	case errors.Is(err, ticket.ErrInvalidTransition):
		allowed := []string{}
		for _, a := range ticket.AllowedFrom(from) {
			allowed = append(allowed, string(a))
		}
		writeProblemFull(w, Problem{Type: "https://errors.cortex9.dev/invalid-transition", Title: "Conflict", Status: http.StatusConflict,
			Detail: err.Error(), Allowed: allowed})
		return
	case err != nil:
		s.storeError(w, err)
		return
	}
	s.cache.Invalidate(id)
	s.kick()
	w.Header().Set("ETag", etag(out.Version))
	writeJSON(w, http.StatusOK, out)
}
