// Package observability define las métricas Prometheus (método RED más
// señales de saturación de cada patrón) y el setup de trazas OpenTelemetry.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Buckets de latencia HTTP en segundos. Incluyen los umbrales de los SLO
// (50ms, 100ms, 250ms) como bordes exactos: así el SLI "porcentaje de
// requests bajo X" se calcula sin interpolar.
var HTTPBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

// Metrics agrupa todas las métricas del servicio.
type Metrics struct {
	Registry *prometheus.Registry

	// RED
	HTTPRequests *prometheus.CounterVec   // rate + errors (label code)
	HTTPDuration *prometheus.HistogramVec // duration
	HTTPInFlight prometheus.Gauge

	RateLimited   *prometheus.CounterVec
	Shed          prometheus.Counter
	ConcLimit     prometheus.Gauge
	Idempotency   *prometheus.CounterVec
	Cache         *prometheus.CounterVec
	OutboxPending prometheus.Gauge
	OutboxLag     prometheus.Gauge
	OutboxPub     prometheus.Counter
	OutboxFail    prometheus.Counter
	QueueDepth    *prometheus.GaugeVec
	QueueEvents   *prometheus.CounterVec
	QueueDuration *prometheus.HistogramVec
	BreakerState  *prometheus.GaugeVec
	BreakerTrans  *prometheus.CounterVec
	Downstream    *prometheus.CounterVec
	Triage        *prometheus.CounterVec
	TriageDur     prometheus.Histogram
	DBPoolInUse   prometheus.Gauge
	TicketsTotal  prometheus.Gauge
}

// NewMetrics registra todo en un registry propio (no el global): tests
// aislados y sin colisiones.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{Registry: r}
	f := func(c prometheus.Collector) { r.MustRegister(c) }

	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total", Help: "HTTP requests by route, method and status code.",
	}, []string{"route", "method", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: HTTPBuckets,
	}, []string{"route", "method"})
	m.HTTPInFlight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "http_requests_in_flight", Help: "In-flight HTTP requests."})
	m.RateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ratelimit_rejected_total", Help: "Requests rejected by the per-client token bucket."}, []string{"route"})
	m.Shed = prometheus.NewCounter(prometheus.CounterOpts{Name: "loadshed_rejected_total", Help: "Requests shed by the adaptive concurrency limiter."})
	m.ConcLimit = prometheus.NewGauge(prometheus.GaugeOpts{Name: "loadshed_concurrency_limit", Help: "Current AIMD concurrency limit."})
	m.Idempotency = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "idempotency_requests_total", Help: "Idempotency-Key outcomes."}, []string{"outcome"})
	m.Cache = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_requests_total", Help: "Read cache results."}, []string{"result"})
	m.OutboxPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_pending_events", Help: "Events waiting in the outbox."})
	m.OutboxLag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_oldest_pending_age_seconds", Help: "Age of the oldest pending outbox event."})
	m.OutboxPub = prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_published_total", Help: "Outbox events published to the queue."})
	m.OutboxFail = prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_publish_failures_total", Help: "Outbox publish attempts that failed (queue full)."})
	m.QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_depth", Help: "Messages in the work queue by state."}, []string{"state"})
	m.QueueEvents = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "queue_messages_total", Help: "Queue message outcomes."}, []string{"type", "event"})
	m.QueueDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "queue_handler_duration_seconds", Help: "Worker handler latency.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 12)}, []string{"type"})
	m.BreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "circuit_breaker_state", Help: "0=closed 1=half_open 2=open."}, []string{"name"})
	m.BreakerTrans = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "circuit_breaker_transitions_total", Help: "Breaker state transitions."}, []string{"name", "from", "to"})
	m.Downstream = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "downstream_requests_total", Help: "Calls to downstream services."}, []string{"service", "result"})
	m.Triage = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "triage_results_total", Help: "Ticket triage by source (agent or fallback reason)."}, []string{"source"})
	m.TriageDur = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "triage_duration_seconds", Help: "Triage latency including fallback.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12)})
	m.DBPoolInUse = prometheus.NewGauge(prometheus.GaugeOpts{Name: "db_pool_in_use", Help: "Simulated DB connections in use."})
	m.TicketsTotal = prometheus.NewGauge(prometheus.GaugeOpts{Name: "tickets_stored", Help: "Tickets in the store."})

	for _, c := range []prometheus.Collector{m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight, m.RateLimited, m.Shed, m.ConcLimit,
		m.Idempotency, m.Cache, m.OutboxPending, m.OutboxLag, m.OutboxPub, m.OutboxFail, m.QueueDepth, m.QueueEvents,
		m.QueueDuration, m.BreakerState, m.BreakerTrans, m.Downstream, m.Triage, m.TriageDur, m.DBPoolInUse, m.TicketsTotal} {
		f(c)
	}
	return m
}
