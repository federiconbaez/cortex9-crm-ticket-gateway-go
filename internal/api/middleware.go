package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/idempotency"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/limiter"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ratelimit"
)

type ctxKey int

const clientKey ctxKey = 0

func clientFrom(ctx context.Context) string {
	s, _ := ctx.Value(clientKey).(string)
	return s
}

// statusRecorder captura status y (opcionalmente) el body.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	wrote   bool
	capture *bytes.Buffer
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wrote {
		return
	}
	r.status, r.wrote = code, true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	if r.capture != nil {
		r.capture.Write(b)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("hijack not supported")
}

// observe es el middleware más externo: métricas RED + span de servidor +
// recover de panics + log de errores/lentos. Usa r.Pattern (la plantilla de
// ruta del ServeMux, p. ej. "GET /v1/tickets/{id}") como label: cardinalidad
// acotada, nunca el path crudo con IDs.
func (s *Server) observe(next http.Handler) http.Handler {
	prop := otel.GetTextMapPropagator()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := s.tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
		r2 := r.WithContext(ctx)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		s.m.HTTPInFlight.Inc()
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler {
					panic(p)
				}
				s.log.Error("panic", "err", p, "path", r.URL.Path)
				if !rec.wrote {
					writeProblem(rec, http.StatusInternalServerError, "internal", "unexpected error")
				}
				rec.status = http.StatusInternalServerError
			}
			s.m.HTTPInFlight.Dec()
			route := r2.Pattern
			if route == "" {
				route = "unmatched"
			}
			el := time.Since(start)
			code := strconv.Itoa(rec.status)
			s.m.HTTPRequests.WithLabelValues(route, r.Method, code).Inc()
			obs := s.m.HTTPDuration.WithLabelValues(route, r.Method)
			sc := span.SpanContext()
			if eo, ok := obs.(prometheus.ExemplarObserver); ok && sc.IsSampled() {
				eo.ObserveWithExemplar(el.Seconds(), prometheus.Labels{"trace_id": sc.TraceID().String()})
			} else {
				obs.Observe(el.Seconds())
			}
			span.SetName(route)
			span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", rec.status),
				attribute.String("client.id", r2.Header.Get("X-Client-ID")))
			if rec.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(rec.status))
			}
			span.End()
			if rec.status >= 500 || el > s.cfg.SlowLog {
				s.log.Warn("request", "route", route, "status", rec.status, "dur_ms", el.Milliseconds(), "trace_id", sc.TraceID().String())
			}
		}()
		next.ServeHTTP(rec, r2)
	})
}

var clientRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// withClient exige X-Client-ID (en producción saldría del token OAuth).
func withClient(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := r.Header.Get("X-Client-ID")
		if !clientRe.MatchString(c) {
			writeProblem(w, http.StatusBadRequest, "client-id", "X-Client-ID header must match [A-Za-z0-9_-]{1,64}")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientKey, c)))
	})
}

// withRateLimit aplica el token bucket por cliente y escribe los headers
// RateLimit-* (draft-ietf-httpapi-ratelimit-headers) y Retry-After.
func (s *Server) withRateLimit(rl *ratelimit.Limiter, next http.Handler) http.Handler {
	policy := rl.Policy()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := rl.Allow(clientFrom(r.Context()))
		h := w.Header()
		h.Set("RateLimit-Policy", policy)
		h.Set("RateLimit-Limit", strconv.Itoa(d.Limit))
		h.Set("RateLimit-Remaining", strconv.Itoa(d.Remaining))
		h.Set("RateLimit-Reset", strconv.Itoa(ratelimit.CeilSeconds(d.Reset)))
		if !d.Allowed {
			h.Set("Retry-After", strconv.Itoa(ratelimit.CeilSeconds(d.RetryAfter)))
			s.m.RateLimited.WithLabelValues(r.Pattern).Inc()
			writeProblem(w, http.StatusTooManyRequests, "rate-limited", "per-client token bucket exhausted")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withShedding aplica el limitador de concurrencia adaptativo.
func (s *Server) withShedding(l *limiter.AIMD, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		release, ok := l.Acquire()
		s.m.ConcLimit.Set(float64(l.Limit()))
		if !ok {
			s.m.Shed.Inc()
			w.Header().Set("Retry-After", "1")
			writeProblem(w, http.StatusServiceUnavailable, "overloaded", "adaptive concurrency limit reached, retry later")
			return
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			switch {
			case rec.status == http.StatusServiceUnavailable || errors.Is(r.Context().Err(), context.DeadlineExceeded):
				release(limiter.Dropped)
			case rec.status >= 400 && rec.status < 500:
				release(limiter.Ignore)
			default:
				release(limiter.Success)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// withTimeout acota el tiempo total del handler (presupuesto por capa).
func withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// replayHeaders son los headers que se guardan para un replay idempotente.
var replayHeaders = []string{"Content-Type", "Location", "ETag"}

// withIdempotency implementa Idempotency-Key para POST/PATCH.
func (s *Server) withIdempotency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			if s.cfg.RequireIdempotencyKey && r.Method == http.MethodPost {
				writeProblem(w, http.StatusBadRequest, "idempotency-key-required", "Idempotency-Key header is required")
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if len(key) > 255 {
			writeProblem(w, http.StatusBadRequest, "idempotency-key", "Idempotency-Key must have at most 255 chars")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeProblem(w, http.StatusRequestEntityTooLarge, "body-too-large", err.Error())
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fp := idempotency.FingerprintOf(r.Method, r.URL.Path, body)
		scoped := clientFrom(r.Context()) + "|" + key
		for {
			res := s.idem.Begin(scoped, fp)
			s.m.Idempotency.WithLabelValues(res.Outcome.String()).Inc()
			switch res.Outcome {
			case idempotency.Replay:
				for k, v := range res.Response.Header {
					w.Header()[k] = v
				}
				w.Header().Set("Idempotent-Replayed", "true")
				w.WriteHeader(res.Response.Status)
				_, _ = w.Write(res.Response.Body)
				return
			case idempotency.Mismatch:
				writeProblem(w, http.StatusUnprocessableEntity, "idempotency-key-reused", "Idempotency-Key was already used with a different request payload")
				return
			case idempotency.InFlight:
				if s.cfg.IdempotencyWait <= 0 {
					w.Header().Set("Retry-After", "1")
					writeProblem(w, http.StatusConflict, "idempotency-in-flight", "a request with this Idempotency-Key is still being processed")
					return
				}
				t := time.NewTimer(s.cfg.IdempotencyWait)
				select {
				case <-res.Ready:
					t.Stop()
					continue // re-evaluamos: replay o, si abortó, la tomamos
				case <-t.C:
				case <-r.Context().Done():
					t.Stop()
				}
				w.Header().Set("Retry-After", "1")
				writeProblem(w, http.StatusConflict, "idempotency-in-flight", "a request with this Idempotency-Key is still being processed")
				return
			}
			// Started: ejecutamos y capturamos.
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK, capture: &bytes.Buffer{}}
			completed := false
			defer func() {
				if !completed {
					s.idem.Abort(scoped, res.Token)
				}
			}()
			next.ServeHTTP(rec, r)
			// 5xx y 429 son reintentables; 409 y 412 dependen del estado
			// concurrente del recurso. Ninguno se memoriza: se libera la key.
			if rec.status >= 500 || rec.status == http.StatusTooManyRequests || rec.status == http.StatusConflict || rec.status == http.StatusPreconditionFailed {
				return
			}
			hdr := http.Header{}
			for _, k := range replayHeaders {
				if v := w.Header().Get(k); v != "" {
					hdr.Set(k, v)
				}
			}
			completed = s.idem.Complete(scoped, res.Token, idempotency.Response{Status: rec.status, Header: hdr, Body: rec.capture.Bytes()})
			if !completed {
				s.log.Warn("idempotency lease lost", "key", key)
				completed = true
			}
			return
		}
	})
}

// logger por defecto descartando.
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
