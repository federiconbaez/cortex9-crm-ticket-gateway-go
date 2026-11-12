package app

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config del proceso completo. Todo se puede sobreescribir por entorno.
type Config struct {
	Addr      string
	AdminAddr string

	RequestTimeout  time.Duration
	ReadHeaderTO    time.Duration
	ReadTO          time.Duration
	WriteTO         time.Duration
	IdleTO          time.Duration
	ShutdownTimeout time.Duration
	PreStopDelay    time.Duration

	RatePerSec float64
	RateBurst  int

	ShedInitial int
	ShedMin     int
	ShedMax     int
	ShedTarget  time.Duration

	IdemTTL   time.Duration
	IdemWait  time.Duration
	IdemLease time.Duration

	CacheTTL  time.Duration
	CacheBeta float64

	DBConns   int
	DBLatency time.Duration

	Workers        int
	QueueCapacity  int
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	HandlerTimeout time.Duration
	OutboxInterval time.Duration

	NotifyTimeout   time.Duration
	NotifyLatency   time.Duration
	NotifyErrorRate float64
	TriageTimeout   time.Duration
	TriageLatency   time.Duration
	TriageErrorRate float64
	TriageMinConf   float64

	TraceExporter string
	TraceFile     string
	OTLPEndpoint  string
	TraceSample   float64
	LogLevel      string
}

// Defaults son valores razonables para un pod chico.
func Defaults() Config {
	return Config{
		Addr: ":8080", AdminAddr: ":8081",
		RequestTimeout: 2 * time.Second, ReadHeaderTO: 2 * time.Second, ReadTO: 5 * time.Second,
		WriteTO: 10 * time.Second, IdleTO: 60 * time.Second, ShutdownTimeout: 25 * time.Second,
		RatePerSec: 1000, RateBurst: 2000,
		ShedInitial: 256, ShedMin: 16, ShedMax: 2048, ShedTarget: 100 * time.Millisecond,
		IdemTTL: 24 * time.Hour, IdemLease: 30 * time.Second,
		CacheTTL: 5 * time.Second, CacheBeta: 1,
		Workers: 8, QueueCapacity: 50000, MaxAttempts: 6, BaseBackoff: 100 * time.Millisecond, MaxBackoff: 10 * time.Second,
		HandlerTimeout: 3 * time.Second, OutboxInterval: 50 * time.Millisecond,
		NotifyTimeout: 300 * time.Millisecond, NotifyLatency: 5 * time.Millisecond,
		TriageTimeout: 500 * time.Millisecond, TriageLatency: 20 * time.Millisecond, TriageMinConf: 0.6,
		TraceExporter: "none", TraceSample: 0.1, LogLevel: "info",
	}
}

// FromEnv aplica variables de entorno sobre Defaults.
func FromEnv(get func(string) string) (Config, error) {
	if get == nil {
		get = os.Getenv
	}
	c := Defaults()
	var errs []error
	str := func(k string, p *string) {
		if v := get(k); v != "" {
			*p = v
		}
	}
	dur := func(k string, p *time.Duration) {
		if v := get(k); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", k, err))
			}
			*p = d
		}
	}
	num := func(k string, p *int) {
		if v := get(k); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", k, err))
			}
			*p = n
		}
	}
	flt := func(k string, p *float64) {
		if v := get(k); v != "" {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", k, err))
			}
			*p = f
		}
	}
	str("ADDR", &c.Addr)
	str("ADMIN_ADDR", &c.AdminAddr)
	dur("REQUEST_TIMEOUT", &c.RequestTimeout)
	dur("SHUTDOWN_TIMEOUT", &c.ShutdownTimeout)
	dur("PRESTOP_DELAY", &c.PreStopDelay)
	flt("RATE_PER_SEC", &c.RatePerSec)
	num("RATE_BURST", &c.RateBurst)
	num("SHED_INITIAL", &c.ShedInitial)
	num("SHED_MIN", &c.ShedMin)
	num("SHED_MAX", &c.ShedMax)
	dur("SHED_TARGET", &c.ShedTarget)
	dur("IDEMPOTENCY_TTL", &c.IdemTTL)
	dur("IDEMPOTENCY_WAIT", &c.IdemWait)
	dur("CACHE_TTL", &c.CacheTTL)
	flt("CACHE_BETA", &c.CacheBeta)
	num("DB_CONNS", &c.DBConns)
	dur("DB_LATENCY", &c.DBLatency)
	num("WORKERS", &c.Workers)
	num("QUEUE_CAPACITY", &c.QueueCapacity)
	num("MAX_ATTEMPTS", &c.MaxAttempts)
	dur("BASE_BACKOFF", &c.BaseBackoff)
	dur("MAX_BACKOFF", &c.MaxBackoff)
	dur("NOTIFY_TIMEOUT", &c.NotifyTimeout)
	dur("NOTIFY_LATENCY", &c.NotifyLatency)
	flt("NOTIFY_ERROR_RATE", &c.NotifyErrorRate)
	dur("TRIAGE_TIMEOUT", &c.TriageTimeout)
	dur("TRIAGE_LATENCY", &c.TriageLatency)
	flt("TRIAGE_ERROR_RATE", &c.TriageErrorRate)
	str("TRACE_EXPORTER", &c.TraceExporter)
	str("TRACE_FILE", &c.TraceFile)
	str("OTLP_ENDPOINT", &c.OTLPEndpoint)
	flt("TRACE_SAMPLE", &c.TraceSample)
	str("LOG_LEVEL", &c.LogLevel)
	if len(errs) > 0 {
		return c, fmt.Errorf("config: %v", errs)
	}
	return c, nil
}
