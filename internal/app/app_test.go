package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/queue"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
)

func contextWithTimeout(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	_ = cancel // el deadline libera los recursos
	return ctx
}

func TestEndToEndCreateTriageNotify(t *testing.T) {
	h := newHarness(t, nil)
	r := h.do("POST", "/v1/tickets", "acme", map[string]any{"customer_id": "c1", "subject": "No reconozco este cobro, creo que es fraude", "channel": "app"}, nil)
	if r.Code != 201 || r.Header.Get("Location") == "" || r.Header.Get("ETag") != `"v1"` {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	var tk ticket.Ticket
	r.json(t, &tk)
	if tk.Priority != ticket.P1 || tk.PrioritySource != "rules" || tk.Status != ticket.StatusOpen {
		t.Fatalf("%+v", tk)
	}
	waitUntil(t, 3*time.Second, func() bool { return h.a.Notified() == 1 })
	g := h.do("GET", "/v1/tickets/"+tk.ID, "acme", nil, nil)
	var got ticket.Ticket
	g.json(t, &got)
	if got.Category != "fraud" || got.TriagedBy == "" || got.Version != 2 {
		t.Fatalf("not triaged: %+v", got)
	}
	if h.a.Store.OutboxLen() != 0 {
		t.Fatal("outbox not drained")
	}
}

func TestClientPriorityIsRespectedByTriage(t *testing.T) {
	h := newHarness(t, nil)
	body := newTicketBody("Mi paquete no llegó, es urgente")
	body["priority"] = "P4"
	r := h.do("POST", "/v1/tickets", "acme", body, nil)
	var tk ticket.Ticket
	r.json(t, &tk)
	waitUntil(t, 3*time.Second, func() bool { return h.a.Notified() == 1 })
	got, _ := h.a.Store.Get(context.Background(), tk.ID)
	if got.Priority != ticket.P4 || got.PrioritySource != "client" || got.Category != "shipping" {
		t.Fatalf("%+v", got)
	}
}

func TestIdempotencyOverHTTP(t *testing.T) {
	h := newHarness(t, nil)
	key := map[string]string{"Idempotency-Key": "abc-123"}
	a := h.do("POST", "/v1/tickets", "acme", newTicketBody("hola"), key)
	b := h.do("POST", "/v1/tickets", "acme", newTicketBody("hola"), key)
	if a.Code != 201 || b.Code != 201 || string(a.Body) != string(b.Body) || b.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay mismatch: %d %d\n%s\n%s", a.Code, b.Code, a.Body, b.Body)
	}
	if b.Header.Get("Location") != a.Header.Get("Location") || h.a.Store.Count() != 1 {
		t.Fatal("side effect executed twice")
	}
	c := h.do("POST", "/v1/tickets", "acme", newTicketBody("otro body"), key)
	if c.Code != 422 {
		t.Fatalf("want 422 got %d", c.Code)
	}
	// la misma key de otro cliente es independiente (scope por tenant)
	if d := h.do("POST", "/v1/tickets", "globex", newTicketBody("otro body"), key); d.Code != 201 {
		t.Fatal(d.Code)
	}
	// un 422 de validación se memoriza (determinista para ese body)
	bad := map[string]string{"Idempotency-Key": "bad-1"}
	if e := h.do("POST", "/v1/tickets", "acme", map[string]any{"subject": "x", "channel": "chat"}, bad); e.Code != 422 {
		t.Fatal(e.Code)
	}
	if e := h.do("POST", "/v1/tickets", "acme", map[string]any{"subject": "x", "channel": "chat"}, bad); e.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatal("4xx should be replayed")
	}
	long := map[string]string{"Idempotency-Key": strings.Repeat("k", 256)}
	if e := h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), long); e.Code != 400 {
		t.Fatal(e.Code)
	}
}

func TestIdempotencyInFlight409AndWait(t *testing.T) {
	for _, wait := range []time.Duration{0, 2 * time.Second} {
		t.Run(fmt.Sprint("wait=", wait), func(t *testing.T) {
			h := newHarness(t, func(c *Config) {
				c.DBConns, c.DBLatency, c.IdemWait = 1, 150*time.Millisecond, wait
			})
			key := map[string]string{"Idempotency-Key": "same"}
			var wg sync.WaitGroup
			codes := make([]int, 2)
			replayed := make([]string, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if i == 1 {
						time.Sleep(30 * time.Millisecond)
					}
					r := h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), key)
					codes[i], replayed[i] = r.Code, r.Header.Get("Idempotent-Replayed")
				}(i)
			}
			wg.Wait()
			if codes[0] != 201 {
				t.Fatal(codes)
			}
			if wait == 0 && codes[1] != 409 {
				t.Fatalf("want 409, got %v", codes)
			}
			if wait > 0 && (codes[1] != 201 || replayed[1] != "true") {
				t.Fatalf("want waited replay, got %v %v", codes, replayed)
			}
			if h.a.Store.Count() != 1 {
				t.Fatal("double create")
			}
		})
	}
}

func TestRateLimitHeaders(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.RatePerSec, c.RateBurst = 1, 3 })
	for i := 0; i < 3; i++ {
		r := h.do("GET", "/v1/tickets", "acme", nil, nil)
		if r.Code != 200 || r.Header.Get("RateLimit-Remaining") != fmt.Sprint(2-i) || r.Header.Get("RateLimit-Limit") != "3" {
			t.Fatalf("i=%d %d %v", i, r.Code, r.Header)
		}
	}
	r := h.do("GET", "/v1/tickets", "acme", nil, nil)
	if r.Code != 429 || r.Header.Get("Retry-After") != "1" || r.Header.Get("RateLimit-Policy") != "3;w=3" {
		t.Fatalf("%d %v", r.Code, r.Header)
	}
	if h.do("GET", "/v1/tickets", "other", nil, nil).Code != 200 {
		t.Fatal("noisy neighbour affected another tenant")
	}
}

func TestLoadSheddingUnderSaturation(t *testing.T) {
	h := newHarness(t, func(c *Config) {
		c.DBConns, c.DBLatency = 2, 80*time.Millisecond
		c.ShedInitial, c.ShedMin, c.ShedMax = 2, 2, 2
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[int]int{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := h.do("GET", "/v1/tickets", "acme", nil, nil)
			mu.Lock()
			codes[r.Code]++
			mu.Unlock()
			if r.Code == 503 && r.Header.Get("Retry-After") == "" {
				t.Error("503 without Retry-After")
			}
		}()
	}
	wg.Wait()
	if codes[503] == 0 || codes[200] == 0 || codes[200]+codes[503] != 12 {
		t.Fatalf("codes %v", codes)
	}
}

func TestRequestTimeoutReturns503(t *testing.T) {
	h := newHarness(t, func(c *Config) {
		c.DBConns, c.DBLatency, c.RequestTimeout = 1, 300*time.Millisecond, 50*time.Millisecond
	})
	start := time.Now()
	r := h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), map[string]string{"Idempotency-Key": "t1"})
	if r.Code != 503 || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("%d after %v", r.Code, time.Since(start))
	}
	// el 503 no se memorizó: la key quedó libre para reintentar
	h.a.Cfg.RequestTimeout = time.Second
	if h.a.Store.Count() != 0 {
		t.Fatal("ticket created despite timeout")
	}
}

func TestPaginationAndTenantIsolation(t *testing.T) {
	h := newHarness(t, nil)
	var ids []string
	for i := 0; i < 7; i++ {
		r := h.do("POST", "/v1/tickets", "acme", newTicketBody(fmt.Sprint("t", i)), nil)
		var tk ticket.Ticket
		r.json(t, &tk)
		ids = append(ids, tk.ID)
		h.do("POST", "/v1/tickets", "globex", newTicketBody("g"), nil)
	}
	var seen []string
	path := "/v1/tickets?limit=3"
	for path != "" {
		r := h.do("GET", path, "acme", nil, nil)
		var page struct {
			Items      []ticket.Ticket `json:"items"`
			NextCursor string          `json:"next_cursor"`
		}
		r.json(t, &page)
		for _, it := range page.Items {
			seen = append(seen, it.ID)
		}
		path = ""
		if link := r.Header.Get("Link"); link != "" {
			path = regexp.MustCompile(`<([^>]+)>`).FindStringSubmatch(link)[1]
		}
	}
	if strings.Join(seen, ",") != strings.Join(ids, ",") {
		t.Fatalf("pagination order/coverage\n%v\n%v", seen, ids)
	}
	if r := h.do("GET", "/v1/tickets/"+ids[0], "globex", nil, nil); r.Code != 404 {
		t.Fatal("cross-tenant read", r.Code)
	}
	if r := h.do("PATCH", "/v1/tickets/"+ids[0]+"/status", "globex", map[string]string{"status": "in_progress"}, nil); r.Code != 404 {
		t.Fatal("cross-tenant write", r.Code)
	}
	for _, p := range []string{"/v1/tickets?cursor=zzz", "/v1/tickets?limit=0", "/v1/tickets?limit=x", "/v1/tickets?status=bogus"} {
		if r := h.do("GET", p, "acme", nil, nil); r.Code != 400 {
			t.Errorf("%s: %d", p, r.Code)
		}
	}
	r := h.do("GET", "/v1/tickets?status=open&limit=2", "acme", nil, nil)
	if !strings.Contains(r.Header.Get("Link"), "status=open") {
		t.Fatal(r.Header.Get("Link"))
	}
}

func TestStateMachineOverHTTP(t *testing.T) {
	h := newHarness(t, nil)
	r := h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), nil)
	var tk ticket.Ticket
	r.json(t, &tk)
	p := "/v1/tickets/" + tk.ID + "/status"
	waitUntil(t, 3*time.Second, func() bool { return h.a.Notified() == 1 }) // triage sube la versión a 2
	bad := h.do("PATCH", p, "acme", map[string]string{"status": "resolved"}, nil)
	var prob struct {
		Status  int      `json:"status"`
		Allowed []string `json:"allowed"`
	}
	bad.json(t, &prob)
	if bad.Code != 409 || strings.Join(prob.Allowed, ",") != "in_progress,closed" || bad.Header.Get("Content-Type") != "application/problem+json" {
		t.Fatalf("%d %s", bad.Code, bad.Body)
	}
	if r := h.do("PATCH", p, "acme", map[string]string{"status": "in_progress"}, map[string]string{"If-Match": `"v1"`}); r.Code != 412 {
		t.Fatal("stale If-Match", r.Code)
	}
	if r := h.do("PATCH", p, "acme", map[string]string{"status": "in_progress"}, map[string]string{"If-Match": "garbage"}); r.Code != 400 {
		t.Fatal(r.Code)
	}
	if r := h.do("PATCH", p, "acme", map[string]string{"status": "flying"}, nil); r.Code != 422 {
		t.Fatal(r.Code)
	}
	if r := h.do("PATCH", "/v1/tickets/nope/status", "acme", map[string]string{"status": "closed"}, nil); r.Code != 404 {
		t.Fatal(r.Code)
	}
	// cache: miss -> hit -> invalidado por el PATCH
	g1 := h.do("GET", "/v1/tickets/"+tk.ID, "acme", nil, nil)
	g2 := h.do("GET", "/v1/tickets/"+tk.ID, "acme", nil, nil)
	if g2.Header.Get("X-Cache") != "hit" {
		t.Fatal("expected cache hit", g1.Header.Get("X-Cache"), g2.Header.Get("X-Cache"))
	}
	if r := h.do("GET", "/v1/tickets/"+tk.ID, "acme", nil, map[string]string{"If-None-Match": g2.Header.Get("ETag")}); r.Code != 304 {
		t.Fatal("conditional GET", r.Code)
	}
	ok := h.do("PATCH", p, "acme", map[string]string{"status": "in_progress"}, map[string]string{"If-Match": `"v2"`})
	if ok.Code != 200 || ok.Header.Get("ETag") != `"v3"` {
		t.Fatalf("%d %s", ok.Code, ok.Body)
	}
	g3 := h.do("GET", "/v1/tickets/"+tk.ID, "acme", nil, nil)
	var after ticket.Ticket
	g3.json(t, &after)
	if after.Status != ticket.StatusInProgress || g3.Header.Get("X-Cache") == "hit" {
		t.Fatal("stale cache after write")
	}
	waitUntil(t, 3*time.Second, func() bool { return h.a.Notified() == 2 })
}

func TestInputValidation(t *testing.T) {
	h := newHarness(t, nil)
	cases := []struct {
		client string
		body   any
		code   int
	}{
		{"", newTicketBody("x"), 400},
		{"bad client!", newTicketBody("x"), 400},
		{"acme", "{not json", 400},
		{"acme", `{"customer_id":"c","subject":"s","channel":"chat","admin":true}`, 400},
		{"acme", map[string]any{"customer_id": "c", "subject": "s", "channel": "fax"}, 422},
		{"acme", map[string]any{"customer_id": "c", "subject": "s", "channel": "chat", "body": strings.Repeat("a", 70000)}, 413},
	}
	for i, c := range cases {
		if r := h.do("POST", "/v1/tickets", c.client, c.body, nil); r.Code != c.code {
			t.Errorf("case %d: want %d got %d %s", i, c.code, r.Code, r.Body)
		}
	}
	if r := h.do("GET", "/v1/tickets/missing", "acme", nil, nil); r.Code != 404 {
		t.Fatal(r.Code)
	}
	if r := h.do("GET", "/nope", "acme", nil, nil); r.Code != 404 {
		t.Fatal(r.Code)
	}
	if r := h.do("GET", "/healthz", "", nil, nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
	if r := h.do("GET", "/readyz", "", nil, nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
}

// Chaos test: con el notificador fallando al 100% y el agente de IA más
// lento que su timeout, la API debe seguir cumpliendo su SLO (todo 201 y
// latencia baja) porque ambos están desacoplados por outbox + cola, con
// breaker y fallback. Además el breaker abre, los mensajes van a la DLQ y
// el redrive los recupera cuando el downstream vuelve.
func TestChaosAPIMeetsSLOWhileDownstreamsFail(t *testing.T) {
	h := newHarness(t, func(c *Config) {
		c.MaxAttempts = 3
		c.TriageTimeout = 30 * time.Millisecond
		c.NotifyErrorRate = 1
		c.TriageLatency = 200 * time.Millisecond
	})
	const n = 300
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		r := h.do("POST", "/v1/tickets", "acme", newTicketBody(fmt.Sprint("cobro duplicado ", i)), map[string]string{"Idempotency-Key": fmt.Sprint("chaos-", i)})
		lat = append(lat, time.Since(start))
		if r.Code != 201 {
			t.Fatalf("API failed during chaos: %d", r.Code)
		}
	}
	p99 := quantile(lat, 0.99)
	// SLO de latencia de la API: p99 < 250ms (holgado para -race y CI).
	if p99 > 250*time.Millisecond {
		t.Fatalf("p99 %v breaks SLO during chaos", p99)
	}
	waitUntil(t, 10*time.Second, func() bool { return h.a.Queue.Stats().DLQ == n })
	if h.a.NotifyCB.State().String() == "closed" {
		t.Fatal("notifier breaker should not be closed")
	}
	total, _ := h.a.Notifier.Calls()
	if total >= int64(n*3) {
		t.Fatalf("breaker did not shed calls: %d calls for %d msgs x 3 attempts", total, n)
	}
	// todos clasificados por fallback (el agente nunca responde a tiempo)
	var c dtoCount
	c.count(h, "acme")
	if c.fallback != n {
		t.Fatalf("fallback triaged %d of %d", c.fallback, n)
	}
	// recuperación: apagamos el caos, esperamos que el breaker deje probar y redrive
	h.a.Notifier.SetChaos(zeroChaos())
	time.Sleep(2100 * time.Millisecond) // OpenTimeout=2s
	if got := h.a.Queue.Redrive(); got != n {
		t.Fatal(got)
	}
	waitUntil(t, 10*time.Second, func() bool { return h.a.Notified() == n })
	t.Logf("chaos: n=%d api_p99=%v notifier_calls=%d (sin breaker serían %d)", n, p99, total, n*3)
}

type dtoCount struct{ fallback int }

func (d *dtoCount) count(h *harness, client string) {
	path := "/v1/tickets?limit=100"
	for path != "" {
		r := h.do("GET", path, client, nil, nil)
		var page struct {
			Items      []ticket.Ticket `json:"items"`
			NextCursor string          `json:"next_cursor"`
		}
		r.json(h.t, &page)
		for _, it := range page.Items {
			if strings.HasPrefix(it.TriagedBy, "fallback") {
				d.fallback++
			}
		}
		path = ""
		if page.NextCursor != "" {
			path = "/v1/tickets?limit=100&cursor=" + page.NextCursor
		}
	}
}

func TestHandleRejectsBadMessages(t *testing.T) {
	h := newHarness(t, nil)
	for _, m := range []queue.Message{
		{ID: "1", Type: "ticket.created", Payload: []byte("nope")},
		{ID: "2", Type: "ticket.unknown", Payload: []byte(`{"ticket_id":"x"}`)},
		{ID: "3", Type: "ticket.created", Payload: []byte(`{"ticket_id":"missing"}`)},
	} {
		if err := h.a.Handle(context.Background(), m); !queue.IsPermanent(err) {
			t.Errorf("%s: want permanent, got %v", m.ID, err)
		}
	}
}

// Shutdown ordenado con listeners reales: una request en vuelo termina con
// 201, readiness pasa a 503 durante el PreStopDelay y no queda nada en
// outbox ni en cola.
func TestGracefulShutdownDrainsEverything(t *testing.T) {
	c := testConfig()
	c.DBConns, c.DBLatency = 4, 150*time.Millisecond
	c.PreStopDelay = 100 * time.Millisecond
	c.NotifyLatency = 20 * time.Millisecond
	a, err := New(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	adm, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = a.Serve(ctx, ln, adm); close(served) }()
	base := "http://" + ln.Addr().String()
	waitUntil(t, 2*time.Second, func() bool {
		r, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
	var wg sync.WaitGroup
	codes := make([]int, 8)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, _ := http.NewRequest("POST", base+"/v1/tickets", strings.NewReader(`{"customer_id":"c","subject":"s","channel":"chat"}`))
			req.Header.Set("X-Client-ID", "acme")
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			codes[i] = r.StatusCode
			r.Body.Close()
		}(i)
	}
	time.Sleep(40 * time.Millisecond) // requests en vuelo (DB tarda 150ms)
	cancel()
	time.Sleep(30 * time.Millisecond)
	if r, err := http.Get(base + "/readyz"); err == nil {
		if r.StatusCode != 503 {
			t.Errorf("readyz during preStop = %d", r.StatusCode)
		}
		r.Body.Close()
	}
	wg.Wait()
	<-served
	for i, code := range codes {
		if code != 201 {
			t.Fatalf("in-flight request %d got %d", i, code)
		}
	}
	if a.Store.OutboxLen() != 0 || a.Queue.Stats().Ready+a.Queue.Stats().Delayed != 0 || a.Notified() != int64(len(codes)) {
		t.Fatalf("not drained: outbox=%d queue=%+v notified=%d", a.Store.OutboxLen(), a.Queue.Stats(), a.Notified())
	}
}

func TestAdminEndpoints(t *testing.T) {
	h := newHarness(t, nil)
	adm := httptest.NewServer(h.a.AdminHandler())
	defer adm.Close()
	put := func(body string) int {
		req, _ := http.NewRequest("PUT", adm.URL+"/admin/chaos", strings.NewReader(body))
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if put(`{"target":"notifier","latency_ms":1,"error_rate":0.5}`) != 204 || h.a.Notifier.Chaos().ErrorRate != 0.5 {
		t.Fatal("chaos notifier")
	}
	if put(`{"target":"triage_agent","malformed_rate":1}`) != 204 || h.a.AIAgent.MalformedRate() != 1 {
		t.Fatal("chaos agent")
	}
	if put(`{"target":"db"}`) != 400 || put(`{"target":"notifier","error_rate":2}`) != 400 || put(`nope`) != 400 {
		t.Fatal("chaos validation")
	}
	h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), nil)
	time.Sleep(50 * time.Millisecond)
	h.a.refreshGauges()
	get := func(p string) string {
		r, err := http.Get(adm.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return string(b)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(get("/admin/stats")), &st); err != nil || st["tickets"].(float64) != 1 {
		t.Fatal(st, err)
	}
	if !strings.HasPrefix(get("/admin/dlq"), "[") && !strings.HasPrefix(get("/admin/dlq"), "null") {
		t.Fatal("dlq")
	}
	r, _ := http.Post(adm.URL+"/admin/dlq/redrive", "", nil)
	r.Body.Close()
	m := get("/metrics")
	for _, name := range []string{"http_requests_total", "http_request_duration_seconds_bucket", "triage_results_total", "circuit_breaker_state", "queue_depth"} {
		if !strings.Contains(m, name) {
			t.Errorf("metric %s missing", name)
		}
	}
}

// Todas las métricas usadas por el dashboard de Grafana y por las reglas de
// alerta tienen que existir en el registry. Evita dashboards "en blanco"
// por un rename.
func TestDashboardAndRulesUseExistingMetrics(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	ch := make(chan *prometheus.Desc, 512)
	go func() { a.Metrics.Registry.Describe(ch); close(ch) }()
	nameRe := regexp.MustCompile(`fqName: "([^"]+)"`)
	for d := range ch {
		if m := nameRe.FindStringSubmatch(d.String()); m != nil {
			known[m[1]] = true
		}
	}
	files := []string{"../../deploy/grafana/dashboards/crm-ticket-gateway.json", "../../deploy/prometheus/rules/slo.yml"}
	tokRe := regexp.MustCompile(`\b([a-z_]+(?:_total|_seconds|_bucket|_count|_sum|_state|_depth|_events|_limit|_flight|_in_use|_stored|_age_seconds))\b`)
	for _, f := range files {
		raw, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(f, ".json") {
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatalf("%s invalid json: %v", f, err)
			}
		}
		for _, m := range tokRe.FindAllStringSubmatch(string(raw), -1) {
			name := m[1]
			if strings.HasPrefix(name, "slo_") || strings.HasPrefix(name, "sli_") || strings.Contains(name, ":") {
				continue
			}
			base := name
			for _, suf := range []string{"_bucket", "_count", "_sum"} {
				if strings.HasSuffix(base, suf) && !known[base] {
					base = strings.TrimSuffix(base, suf)
				}
			}
			if !known[base] {
				t.Errorf("%s references unknown metric %q", f, name)
			}
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{"ADDR": ":9", "RATE_PER_SEC": "5", "RATE_BURST": "7", "DB_LATENCY": "2ms", "TRACE_EXPORTER": "stdout"}
	c, err := FromEnv(func(k string) string { return env[k] })
	if err != nil || c.Addr != ":9" || c.RatePerSec != 5 || c.RateBurst != 7 || c.DBLatency != 2*time.Millisecond || c.TraceExporter != "stdout" {
		t.Fatal(c, err)
	}
	if _, err := FromEnv(func(k string) string {
		return map[string]string{"RATE_BURST": "x", "DB_LATENCY": "y", "CACHE_BETA": "z"}[k]
	}); err == nil {
		t.Fatal("bad env accepted")
	}
	if c, err := FromEnv(nil); err != nil || c.Addr == "" {
		t.Fatal(err)
	}
}

func TestTracingStdoutPropagatesToWorker(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "traces.json")
	h := newHarness(t, func(c *Config) { c.TraceExporter, c.TraceFile, c.TraceSample = "stdout", f, 1 })
	h.do("POST", "/v1/tickets", "acme", newTicketBody("x"), map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e9e4736-00f067aa0ba902b7-01"})
	waitUntil(t, 3*time.Second, func() bool { return h.a.Notified() == 1 })
	h.a.Shutdown(contextWithTimeout(5 * time.Second))
	raw, _ := os.ReadFile(f)
	s := string(raw)
	// el span del server y el del worker comparten el trace id entrante
	if strings.Count(s, "4bf92f3577b34da6a3ce929d0e9e4736") < 2 || !strings.Contains(s, "process ticket.created") || !strings.Contains(s, "POST /v1/tickets") {
		t.Fatalf("trace propagation broken:\n%.2000s", s)
	}
	if _, err := New(Config{TraceExporter: "zipkin"}, nil); err == nil {
		t.Fatal("unknown exporter accepted")
	}
	if _, err := New(Config{TraceExporter: "stdout", TraceFile: "/nonexistent/dir/x"}, nil); err == nil {
		t.Fatal("bad trace file accepted")
	}
}
