// Package app arma el proceso: store, caché, colas, workers, breakers,
// servidores HTTP y el apagado ordenado.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/api"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/breaker"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/cache"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/downstream"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/idempotency"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/limiter"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/observability"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/outbox"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/queue"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ratelimit"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/store"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/triage"
)

// App es el proceso armado.
type App struct {
	Cfg      Config
	Log      *slog.Logger
	Metrics  *observability.Metrics
	Store    *store.Store
	Cache    *cache.Cache[string, ticket.Ticket]
	Idem     *idempotency.Store
	RL       *ratelimit.Limiter
	Shedder  *limiter.AIMD
	Queue    *queue.Queue
	Relay    *outbox.Relay
	Notifier *downstream.Sim
	AI       *downstream.Sim
	AIAgent  *triage.Agent
	NotifyCB *breaker.Breaker
	TriageCB *breaker.Breaker
	Triage   *triage.Service
	API      *api.Server
	Tracer   trace.Tracer

	traceShutdown func(context.Context) error
	traceFile     io.Closer
	ready         atomic.Bool
	httpSrv       *http.Server
	adminSrv      *http.Server
	stopBG        chan struct{}
	bgWG          sync.WaitGroup
	notified      atomic.Int64
	shutdownOnce  sync.Once
	shutdownRep   ShutdownReport
}

// New arma la App (sin abrir puertos).
func New(cfg Config, log *slog.Logger) (*App, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	a := &App{Cfg: cfg, Log: log, Metrics: observability.NewMetrics(), stopBG: make(chan struct{})}
	var w io.Writer = os.Stdout
	if cfg.TraceFile != "" {
		f, err := os.Create(cfg.TraceFile)
		if err != nil {
			return nil, err
		}
		a.traceFile, w = f, f
	}
	tr, shut, err := observability.SetupTracing(context.Background(), observability.TracingConfig{
		Exporter: cfg.TraceExporter, Writer: w, OTLPEndpoint: cfg.OTLPEndpoint, SampleRatio: cfg.TraceSample,
	})
	if err != nil {
		return nil, err
	}
	a.Tracer, a.traceShutdown = tr, shut

	a.Store = store.New(store.WithSimulatedDB(cfg.DBConns, cfg.DBLatency))
	a.Cache = cache.New[string, ticket.Ticket](cache.Config{TTL: cfg.CacheTTL, Beta: cfg.CacheBeta, MaxEntries: 100000, LoadTimeout: cfg.RequestTimeout}, cache.StringHash)
	a.Idem = idempotency.New(cfg.IdemTTL, cfg.IdemLease)
	a.RL = ratelimit.New(cfg.RatePerSec, cfg.RateBurst)
	a.Shedder = limiter.New(limiter.Config{Initial: cfg.ShedInitial, Min: cfg.ShedMin, Max: cfg.ShedMax, Target: cfg.ShedTarget, Backoff: 0.9, Cooldown: 100 * time.Millisecond})

	onState := func(name string, from, to breaker.State) {
		a.Metrics.BreakerState.WithLabelValues(name).Set(float64(to))
		a.Metrics.BreakerTrans.WithLabelValues(name, from.String(), to.String()).Inc()
		log.Warn("breaker", "name", name, "from", from.String(), "to", to.String())
	}
	bcfg := breaker.Config{Window: 10 * time.Second, Buckets: 10, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 2 * time.Second, HalfOpenProbes: 3}
	a.NotifyCB = breaker.New("notifier", bcfg)
	a.NotifyCB.OnStateChange = onState
	a.TriageCB = breaker.New("triage_agent", bcfg)
	a.TriageCB.OnStateChange = onState
	a.Metrics.BreakerState.WithLabelValues("notifier").Set(0)
	a.Metrics.BreakerState.WithLabelValues("triage_agent").Set(0)

	a.Notifier = downstream.New("notifier", 42, downstream.Chaos{Latency: cfg.NotifyLatency, Jitter: cfg.NotifyLatency, ErrorRate: cfg.NotifyErrorRate})
	a.AI = downstream.New("triage_agent", 43, downstream.Chaos{Latency: cfg.TriageLatency, Jitter: cfg.TriageLatency, ErrorRate: cfg.TriageErrorRate})
	a.AIAgent = triage.NewAgent(a.AI, 0)
	a.Triage = &triage.Service{Agent: a.AIAgent, Breaker: a.TriageCB, Timeout: cfg.TriageTimeout, MinConfidence: cfg.TriageMinConf,
		OnResult: func(src string, took time.Duration) {
			a.Metrics.Triage.WithLabelValues(src).Inc()
			a.Metrics.TriageDur.Observe(took.Seconds())
		}}

	a.Queue = queue.New(queue.Config{Workers: cfg.Workers, Capacity: cfg.QueueCapacity, MaxAttempts: cfg.MaxAttempts,
		BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, HandlerTimeout: cfg.HandlerTimeout})
	a.Queue.OnEvent = func(e queue.Event, m queue.Message, took time.Duration) {
		a.Metrics.QueueEvents.WithLabelValues(m.Type, string(e)).Inc()
		if e != queue.EvDuplicate {
			a.Metrics.QueueDuration.WithLabelValues(m.Type).Observe(took.Seconds())
		}
	}
	a.Relay = outbox.NewRelay(a.Store, func(e store.OutboxEvent) error {
		return a.Queue.Enqueue(queue.Message{ID: e.ID, Type: e.Type, Rank: e.Priority.Rank(), Payload: e.Payload, Headers: e.Headers})
	}, cfg.OutboxInterval, 512)
	a.Relay.OnFlush = func(p, f int) {
		a.Metrics.OutboxPub.Add(float64(p))
		a.Metrics.OutboxFail.Add(float64(f))
	}

	a.API = api.New(api.Config{RequestTimeout: cfg.RequestTimeout, IdempotencyWait: cfg.IdemWait}, api.Deps{
		Store: a.Store, Cache: a.Cache, Idem: a.Idem, RateLimit: a.RL, Shedder: a.Shedder, Metrics: a.Metrics,
		Tracer: a.Tracer, Log: log, OnCommit: a.Relay.Kick,
	}, a.ready.Load)
	return a, nil
}

// Notified devuelve cuántas notificaciones se entregaron.
func (a *App) Notified() int64 { return a.notified.Load() }

func (a *App) notify(ctx context.Context) error {
	err := a.NotifyCB.Do(func() error {
		cctx, cancel := context.WithTimeout(ctx, a.Cfg.NotifyTimeout)
		defer cancel()
		return a.Notifier.Call(cctx)
	})
	res := "ok"
	switch {
	case errors.Is(err, breaker.ErrOpen):
		res = "rejected_open"
	case err != nil:
		res = "error"
	}
	a.Metrics.Downstream.WithLabelValues("notifier", res).Inc()
	if err == nil {
		a.notified.Add(1)
	}
	return err
}

// Handle es el handler de la cola (exportado para tests).
func (a *App) Handle(ctx context.Context, m queue.Message) error {
	ctx, span := a.Tracer.Start(observability.Extract(ctx, m.Headers), "process "+m.Type, trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	var p struct {
		TicketID string `json:"ticket_id"`
	}
	if err := json.Unmarshal(m.Payload, &p); err != nil || p.TicketID == "" {
		return queue.Permanent(fmt.Errorf("bad payload: %v", err))
	}
	switch m.Type {
	case "ticket.created":
		if err := a.triageTicket(ctx, p.TicketID); err != nil {
			return err
		}
		return a.notify(ctx)
	case "ticket.status_changed":
		return a.notify(ctx)
	default:
		return queue.Permanent(fmt.Errorf("unknown event type %q", m.Type))
	}
}

// triageTicket es idempotente: si el ticket ya fue clasificado no hace nada
// (un reintento por falla de la notificación no vuelve a llamar al agente).
func (a *App) triageTicket(ctx context.Context, id string) error {
	t, err := a.Store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return queue.Permanent(err)
	}
	if err != nil {
		return err
	}
	if t.TriagedBy != "" {
		return nil
	}
	r := a.Triage.Classify(ctx, t.Subject, t.Body)
	err = a.Store.WithTx(ctx, func(tx *store.Tx) error {
		cur, err := tx.GetForUpdate(id)
		if err != nil {
			return err
		}
		if cur.TriagedBy != "" {
			return nil
		}
		ver := cur.Version
		cur.Category, cur.TriagedBy = r.Category, r.Source
		if cur.PrioritySource != "client" && r.Priority != cur.Priority {
			cur.Priority, cur.PrioritySource = r.Priority, r.Source
			cur.DueAt = cur.CreatedAt.Add(r.Priority.SLA())
		}
		cur.Version++
		tx.UpdateTicket(cur, ver)
		return nil
	})
	if err != nil {
		return err
	}
	a.Cache.Invalidate(id)
	return nil
}

// AdminHandler expone /metrics, chaos, DLQ y pprof en un puerto separado
// (nunca expuesto a internet).
func (a *App) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.Metrics.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	type chaosReq struct {
		Target        string  `json:"target"`
		LatencyMS     int     `json:"latency_ms"`
		JitterMS      int     `json:"jitter_ms"`
		ErrorRate     float64 `json:"error_rate"`
		MalformedRate float64 `json:"malformed_rate"`
	}
	mux.HandleFunc("PUT /admin/chaos", func(w http.ResponseWriter, r *http.Request) {
		var c chaosReq
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.ErrorRate < 0 || c.ErrorRate > 1 {
			http.Error(w, "bad chaos request", http.StatusBadRequest)
			return
		}
		ch := downstream.Chaos{Latency: time.Duration(c.LatencyMS) * time.Millisecond, Jitter: time.Duration(c.JitterMS) * time.Millisecond, ErrorRate: c.ErrorRate}
		switch c.Target {
		case "notifier":
			a.Notifier.SetChaos(ch)
		case "triage_agent":
			a.AI.SetChaos(ch)
			a.AIAgent.SetMalformedRate(c.MalformedRate)
		default:
			http.Error(w, "unknown target", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) {
		nt, nf := a.Notifier.Calls()
		at, af := a.AI.Calls()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queue": a.Queue.Stats(), "outbox_pending": a.Store.OutboxLen(), "tickets": a.Store.Count(),
			"notified": a.Notified(), "notifier_calls": nt, "notifier_failed": nf, "agent_calls": at, "agent_failed": af,
			"breakers":          map[string]string{"notifier": a.NotifyCB.State().String(), "triage_agent": a.TriageCB.State().String()},
			"concurrency_limit": a.Shedder.Limit(),
		})
	})
	mux.HandleFunc("GET /admin/dlq", func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(a.Queue.DLQ()) })
	mux.HandleFunc("POST /admin/dlq/redrive", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"redriven": a.Queue.Redrive()})
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// StartBackground arranca workers, relay y tareas periódicas.
func (a *App) StartBackground() {
	a.Queue.Start(a.Handle)
	a.Relay.Start()
	a.bgWG.Add(1)
	go func() {
		defer a.bgWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		gauges := time.NewTicker(250 * time.Millisecond)
		defer gauges.Stop()
		for {
			select {
			case <-a.stopBG:
				return
			case <-gauges.C:
				a.refreshGauges()
			case <-t.C:
				a.RL.Sweep()
				a.Idem.Sweep()
				a.Queue.SweepProcessed()
			}
		}
	}()
	a.ready.Store(true)
}

func (a *App) refreshGauges() {
	s := a.Queue.Stats()
	a.Metrics.QueueDepth.WithLabelValues("ready").Set(float64(s.Ready))
	a.Metrics.QueueDepth.WithLabelValues("delayed").Set(float64(s.Delayed))
	a.Metrics.QueueDepth.WithLabelValues("in_flight").Set(float64(s.InFlight))
	a.Metrics.QueueDepth.WithLabelValues("dlq").Set(float64(s.DLQ))
	a.Metrics.OutboxPending.Set(float64(a.Store.OutboxLen()))
	a.Metrics.OutboxLag.Set(a.Store.OldestPendingAge().Seconds())
	a.Metrics.DBPoolInUse.Set(float64(a.Store.PoolInUse()))
	a.Metrics.TicketsTotal.Set(float64(a.Store.Count()))
	a.Metrics.ConcLimit.Set(float64(a.Shedder.Limit()))
	a.Metrics.BreakerState.WithLabelValues("notifier").Set(float64(a.NotifyCB.State()))
	a.Metrics.BreakerState.WithLabelValues("triage_agent").Set(float64(a.TriageCB.State()))
}

// Serve abre los listeners y bloquea hasta que ctx se cancele; entonces
// ejecuta Shutdown.
func (a *App) Serve(ctx context.Context, ln, adminLn net.Listener) error {
	c := a.Cfg
	a.httpSrv = &http.Server{Handler: a.API, ReadHeaderTimeout: c.ReadHeaderTO, ReadTimeout: c.ReadTO, WriteTimeout: c.WriteTO, IdleTimeout: c.IdleTO,
		BaseContext: func(net.Listener) context.Context { return context.Background() }}
	a.adminSrv = &http.Server{Handler: a.AdminHandler(), ReadHeaderTimeout: c.ReadHeaderTO}
	a.StartBackground()
	errc := make(chan error, 2)
	go func() { errc <- a.httpSrv.Serve(ln) }()
	go func() { errc <- a.adminSrv.Serve(adminLn) }()
	a.Log.Info("listening", "addr", ln.Addr().String(), "admin", adminLn.Addr().String())
	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("server", "err", err)
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
	defer cancel()
	rep := a.Shutdown(sctx)
	a.Log.Info("shutdown complete", "report", rep)
	return nil
}

// ShutdownReport describe qué pasó en el apagado.
type ShutdownReport struct {
	HTTPDrained    bool          `json:"http_drained"`
	OutboxLeft     int           `json:"outbox_left"`
	QueueLeft      int           `json:"queue_left"`
	Took           time.Duration `json:"took"`
	TraceFlushedOK bool          `json:"trace_flushed_ok"`
}

// Shutdown ordenado. El presupuesto total es el deadline de ctx y se reparte
// por etapas:
//  1. readiness=false y PreStopDelay: el balanceador deja de mandarnos tráfico.
//  2. http.Server.Shutdown: deja de aceptar y espera las requests en vuelo.
//  3. relay.Stop: publica en la cola lo que quedó en la outbox.
//  4. queue.Shutdown: drena mensajes (incluidos reintentos ya programados).
//  5. flush de trazas; por último el server admin (métricas hasta el final).
//
// Es idempotente: una segunda llamada (por ejemplo, la señal y el cleanup de
// un test) devuelve el reporte de la primera sin volver a cerrar canales.
func (a *App) Shutdown(ctx context.Context) ShutdownReport {
	a.shutdownOnce.Do(func() { a.shutdownRep = a.shutdown(ctx) })
	return a.shutdownRep
}

func (a *App) shutdown(ctx context.Context) ShutdownReport {
	start := time.Now()
	rep := ShutdownReport{}
	a.ready.Store(false)
	if a.Cfg.PreStopDelay > 0 {
		select {
		case <-time.After(a.Cfg.PreStopDelay):
		case <-ctx.Done():
		}
	}
	if a.httpSrv != nil {
		hctx, cancel := stage(ctx, 0.4)
		rep.HTTPDrained = a.httpSrv.Shutdown(hctx) == nil
		cancel()
	} else {
		rep.HTTPDrained = true
	}
	octx, cancel := stage(ctx, 0.3)
	rep.OutboxLeft = a.Relay.Stop(octx)
	cancel()
	qctx, cancel := stage(ctx, 0.9)
	rep.QueueLeft = a.Queue.Shutdown(qctx)
	cancel()
	close(a.stopBG)
	a.bgWG.Wait()
	a.Cache.Wait()
	rep.TraceFlushedOK = a.traceShutdown(ctx) == nil
	if a.traceFile != nil {
		_ = a.traceFile.Close()
	}
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	rep.Took = time.Since(start)
	return rep
}

// stage devuelve un context con una fracción del tiempo restante.
func stage(ctx context.Context, frac float64) (context.Context, context.CancelFunc) {
	dl, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Duration(float64(time.Until(dl))*frac))
}
