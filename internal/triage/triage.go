// Package triage clasifica tickets (categoría y prioridad) con un "agente de
// IA" SIMULADO y determinista, detrás de un circuit breaker, con timeout,
// validación de la salida (guardrails) y fallback a reglas.
//
// Importante: acá no hay ningún LLM ni llamada de red. El "agente" es una
// función determinista (puntajes por palabras clave + confianza derivada del
// texto) envuelta en un downstream simulado que agrega latencia y fallas.
// Lo que se quiere mostrar es la ingeniería alrededor de un agente real:
// presupuesto de latencia, breaker, salida estructurada validada, umbral de
// confianza y un fallback que nunca falla, para que la IA sea una mejora y
// no un punto único de falla del flujo de atención.
package triage

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/breaker"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/downstream"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
)

// Result de la clasificación.
type Result struct {
	Category   string          `json:"category"`
	Priority   ticket.Priority `json:"priority"`
	Confidence float64         `json:"confidence"`
	Source     string          `json:"source"`
}

// Fuentes posibles (también son la label de la métrica).
const (
	SourceAgent          = "agent"
	SourceFallbackOpen   = "fallback_breaker_open"
	SourceFallbackError  = "fallback_agent_error"
	SourceFallbackBad    = "fallback_invalid_output"
	SourceFallbackLowCnf = "fallback_low_confidence"
)

var categories = map[string]bool{"billing": true, "shipping": true, "account": true, "fraud": true, "general": true}

type rule struct {
	category string
	priority ticket.Priority
	words    []string
}

// Orden = precedencia.
var rules = []rule{
	{"fraud", ticket.P1, []string{"fraude", "fraud", "estafa", "hackearon", "no reconozco", "unauthorized"}},
	{"billing", ticket.P2, []string{"cobro", "cobraron", "factura", "reembolso", "refund", "charge", "tarjeta"}},
	{"shipping", ticket.P2, []string{"envío", "envio", "no llegó", "no llego", "paquete", "shipping", "entrega"}},
	{"account", ticket.P3, []string{"cuenta", "contraseña", "password", "login", "acceso"}},
}

// Rules es el clasificador de fallback: puro, rápido y siempre responde.
func Rules(subject, body string) Result {
	text := strings.ToLower(subject + " " + body)
	for _, r := range rules {
		for _, w := range r.words {
			if strings.Contains(text, w) {
				return Result{Category: r.category, Priority: r.priority, Confidence: 1}
			}
		}
	}
	return Result{Category: "general", Priority: ticket.P3, Confidence: 1}
}

// Agent simula el agente: mismas features + una "confianza" estable por
// texto y, con MalformedRate, una salida inválida (alucinación) para
// ejercitar los guardrails.
type Agent struct {
	Sim       *downstream.Sim
	malformed atomic.Uint64 // bits de un float64 0..1
}

// NewAgent crea el agente simulado. malformedRate (0..1) es la fracción de
// textos (determinista por hash) para los que devuelve salida inválida.
func NewAgent(sim *downstream.Sim, malformedRate float64) *Agent {
	a := &Agent{Sim: sim}
	a.SetMalformedRate(malformedRate)
	return a
}

// SetMalformedRate cambia la tasa de "alucinación" en caliente.
func (a *Agent) SetMalformedRate(r float64) { a.malformed.Store(math.Float64bits(r)) }

// MalformedRate devuelve la tasa actual.
func (a *Agent) MalformedRate() float64 { return math.Float64frombits(a.malformed.Load()) }

// Classify llama al agente simulado.
func (a *Agent) Classify(ctx context.Context, subject, body string) (Result, error) {
	if err := a.Sim.Call(ctx); err != nil {
		return Result{}, err
	}
	base := Rules(subject, body)
	h := fnv.New64a()
	_, _ = h.Write([]byte(subject + "\x00" + body))
	sum := h.Sum64()
	conf := 0.55 + float64(sum%45)/100 // 0.55..0.99
	if mr := a.MalformedRate(); mr > 0 && float64(sum%1000)/1000 < mr {
		return Result{Category: "¯\\_(ツ)_/¯", Priority: "P0", Confidence: conf}, nil
	}
	pr := base.Priority
	// el "agente" detecta urgencia que las reglas no ven
	text := strings.ToLower(subject + " " + body)
	if pr != ticket.P1 && (strings.Contains(text, "urgente") || strings.Contains(text, "urgent")) {
		pr = ticket.Priority("P" + string(rune('0'+pr.Rank())))
	}
	return Result{Category: base.Category, Priority: pr, Confidence: conf}, nil
}

// Validate es el guardrail sobre la salida estructurada.
func Validate(r Result) error {
	if !categories[r.Category] || !ticket.ValidPriority(r.Priority) || r.Confidence < 0 || r.Confidence > 1 {
		return errors.New("invalid agent output")
	}
	return nil
}

// Classifier es la interfaz que ve el agente.
type Classifier interface {
	Classify(ctx context.Context, subject, body string) (Result, error)
}

// Service orquesta agente + breaker + fallback.
type Service struct {
	Agent         Classifier
	Breaker       *breaker.Breaker
	Timeout       time.Duration
	MinConfidence float64
	// OnResult (opcional) para métricas.
	OnResult func(source string, took time.Duration)
}

// Classify nunca devuelve error: si el agente no sirve, usa reglas.
func (s *Service) Classify(ctx context.Context, subject, body string) Result {
	start := time.Now()
	r := s.classify(ctx, subject, body)
	if s.OnResult != nil {
		s.OnResult(r.Source, time.Since(start))
	}
	return r
}

func (s *Service) classify(ctx context.Context, subject, body string) Result {
	fb := func(src string) Result { r := Rules(subject, body); r.Source = src; return r }
	done, err := s.Breaker.Allow()
	if err != nil {
		return fb(SourceFallbackOpen)
	}
	cctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	r, err := s.Agent.Classify(cctx, subject, body)
	if err != nil {
		done(false)
		return fb(SourceFallbackError)
	}
	if Validate(r) != nil {
		// Una salida inválida cuenta como falla del agente para el breaker:
		// si empieza a alucinar en masa, dejamos de llamarlo.
		done(false)
		return fb(SourceFallbackBad)
	}
	done(true)
	if r.Confidence < s.MinConfidence {
		return fb(SourceFallbackLowCnf)
	}
	r.Source = SourceAgent
	return r
}
