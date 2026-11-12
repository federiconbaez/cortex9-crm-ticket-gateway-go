package triage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/breaker"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/downstream"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
)

func TestRules(t *testing.T) {
	cases := []struct {
		s, b, cat string
		p         ticket.Priority
	}{
		{"Me cobraron dos veces", "", "billing", ticket.P2},
		{"No reconozco una compra", "creo que es fraude", "fraud", ticket.P1},
		{"Mi paquete no llegó", "", "shipping", ticket.P2},
		{"Olvidé la contraseña", "", "account", ticket.P3},
		{"Consulta", "hola", "general", ticket.P3},
	}
	for _, c := range cases {
		r := Rules(c.s, c.b)
		if r.Category != c.cat || r.Priority != c.p {
			t.Errorf("%q: %+v", c.s, r)
		}
	}
}

func newSvc(sim *downstream.Sim, malformed float64) *Service {
	return &Service{
		Agent:         NewAgent(sim, malformed),
		Breaker:       breaker.New("triage", breaker.Config{MinRequests: 5, FailureRatio: 0.5, OpenTimeout: time.Hour}),
		Timeout:       20 * time.Millisecond,
		MinConfidence: 0.6,
	}
}

func TestAgentHappyPathAndUrgencyBump(t *testing.T) {
	svc := newSvc(downstream.New("ai", 1, downstream.Chaos{}), 0)
	var sources []string
	svc.OnResult = func(s string, _ time.Duration) { sources = append(sources, s) }
	agentHits := 0
	for i := 0; i < 50; i++ {
		r := svc.Classify(context.Background(), fmt.Sprintf("Mi paquete no llegó #%d", i), "es urgente")
		if r.Source == SourceAgent {
			agentHits++
			if r.Priority != ticket.P1 || r.Category != "shipping" {
				t.Fatalf("%+v", r)
			}
		} else if r.Source != SourceFallbackLowCnf {
			t.Fatalf("unexpected source %s", r.Source)
		}
	}
	if agentHits == 0 || len(sources) != 50 {
		t.Fatal("agent never used")
	}
}

func TestFallbacks(t *testing.T) {
	// timeout => fallback_agent_error, y tras 5 fallas el breaker abre.
	sim := downstream.New("ai", 1, downstream.Chaos{Latency: time.Second})
	svc := newSvc(sim, 0)
	for i := 0; i < 5; i++ {
		if r := svc.Classify(context.Background(), "cobro", ""); r.Source != SourceFallbackError || r.Category != "billing" {
			t.Fatalf("%+v", r)
		}
	}
	start := time.Now()
	r := svc.Classify(context.Background(), "cobro", "")
	if r.Source != SourceFallbackOpen || time.Since(start) > 5*time.Millisecond {
		t.Fatalf("breaker should fail fast: %+v %v", r, time.Since(start))
	}
	calls, _ := sim.Calls()
	if calls != 5 {
		t.Fatalf("agent called while open: %d", calls)
	}
	// salida inválida => guardrail
	svc2 := newSvc(downstream.New("ai", 1, downstream.Chaos{}), 1)
	if r := svc2.Classify(context.Background(), "x", "y"); r.Source != SourceFallbackBad || Validate(r) != nil {
		t.Fatalf("%+v", r)
	}
	if Validate(Result{Category: "billing", Priority: "P1", Confidence: 2}) == nil {
		t.Fatal("confidence > 1 accepted")
	}
}
