// Command loadtest es una prueba de carga corta y reproducible.
//
// Levanta el gateway completo dentro del mismo proceso, en loopback
// (127.0.0.1, puerto efímero), y le aplica carga de "lazo abierto": las
// requests se programan a una tasa fija y la latencia se mide desde el
// instante en que la request DEBÍA salir, no desde que salió. Así se evita
// la "omisión coordinada" (si el server se traba, el generador no baja la
// tasa y las demoras aparecen en los percentiles).
//
// No usa red externa. La semilla del generador es fija; los tiempos
// absolutos dependen de la máquina, por eso el reporte guarda CPU, GOOS y
// versión de Go junto con los números.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/app"
	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/downstream"
)

type scenario struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	RPS         int     `json:"rps"`
	Seconds     int     `json:"seconds"`
	WriteRatio  float64 `json:"write_ratio"`
	DupRatio    float64 `json:"duplicate_post_ratio"`
	Clients     int     `json:"clients"`
	HotRPS      int     `json:"hot_client_rps"`
	ChaosErr    float64 `json:"downstream_error_rate"`
	RatePerSec  float64 `json:"rate_limit_per_client"`
	RateBurst   int     `json:"rate_burst_per_client"`
}

type lat struct {
	P50, P90, P99, P999, Max float64
}

type result struct {
	Scenario       scenario       `json:"scenario"`
	Rep            int            `json:"repetition"`
	WarmupSkipped  int            `json:"warmup_samples_excluded"`
	Sent           int            `json:"sent"`
	Achieved       float64        `json:"achieved_rps"`
	ByCode         map[string]int `json:"by_status_code"`
	TransportErr   int            `json:"transport_errors"`
	LatencyMS      lat            `json:"latency_ms_all"`
	NormalMS       lat            `json:"latency_ms_normal_clients"`
	WriteMS        lat            `json:"latency_ms_post"`
	ReadMS         lat            `json:"latency_ms_get"`
	TriageSources  map[string]int `json:"triage_by_source"`
	HotRejectRatio float64        `json:"hot_client_429_ratio,omitempty"`
	NormalReject   float64        `json:"normal_clients_429_ratio"`
	UniqueKeys     int            `json:"unique_idempotency_keys_ok"`
	TicketsStored  int            `json:"tickets_stored"`
	CacheHitRatio  float64        `json:"cache_hit_ratio"`
	Drain          map[string]any `json:"after_drain"`
}

func quant(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	i := int(q*float64(len(xs))+0.5) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(xs) {
		i = len(xs) - 1
	}
	return xs[i]
}

func summarize(xs []float64) lat {
	sort.Float64s(xs)
	r := func(v float64) float64 { return float64(int(v*1000+0.5)) / 1000 }
	if len(xs) == 0 {
		return lat{}
	}
	return lat{r(quant(xs, .5)), r(quant(xs, .9)), r(quant(xs, .99)), r(quant(xs, .999)), r(xs[len(xs)-1])}
}

func counterSum(fams []*dto.MetricFamily, name string, label, value string) float64 {
	s := 0.0
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.Metric {
			ok := label == ""
			for _, l := range m.Label {
				if l.GetName() == label && l.GetValue() == value {
					ok = true
				}
			}
			if ok && m.Counter != nil {
				s += m.Counter.GetValue()
			}
		}
	}
	return s
}

func triageSources(fams []*dto.MetricFamily) map[string]int {
	out := map[string]int{}
	for _, f := range fams {
		if f.GetName() != "triage_results_total" {
			continue
		}
		for _, m := range f.Metric {
			for _, l := range m.Label {
				if l.GetName() == "source" {
					out[l.GetValue()] += int(m.Counter.GetValue())
				}
			}
		}
	}
	return out
}

// warmup: las respuestas cuya hora programada cae en el primer segundo no
// entran en los percentiles (conexiones TCP nuevas, pools vacíos). Se
// cuentan aparte en WarmupSkipped para que quede a la vista.
const warmup = time.Second

func run(sc scenario, seed uint64) (result, error) {
	cfg := app.Defaults()
	cfg.DBConns, cfg.DBLatency = 64, 2*time.Millisecond
	cfg.NotifyLatency, cfg.TriageLatency = 5*time.Millisecond, 20*time.Millisecond
	cfg.NotifyErrorRate, cfg.TriageErrorRate = sc.ChaosErr, sc.ChaosErr
	cfg.BaseBackoff, cfg.MaxBackoff = 20*time.Millisecond, 200*time.Millisecond
	cfg.RatePerSec, cfg.RateBurst = sc.RatePerSec, sc.RateBurst
	cfg.Workers = 16
	a, err := app.New(cfg, nil)
	if err != nil {
		return result{}, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return result{}, err
	}
	adm, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return result{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = a.Serve(ctx, ln, adm); close(served) }()
	base := "http://" + ln.Addr().String()
	for i := 0; i < 100; i++ { // espera readiness
		if r, err := http.Get(base + "/readyz"); err == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sc.ChaosErr > 0 {
		a.Notifier.SetChaos(downstream.Chaos{Latency: 5 * time.Millisecond, Jitter: 5 * time.Millisecond, ErrorRate: sc.ChaosErr})
		a.AI.SetChaos(downstream.Chaos{Latency: 20 * time.Millisecond, Jitter: 20 * time.Millisecond, ErrorRate: sc.ChaosErr})
	}
	tr := &http.Transport{MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, IdleConnTimeout: 30 * time.Second}
	cl := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	// Plan determinista: qué request sale en cada slot.
	type req struct {
		at     time.Duration
		client string
		hot    bool
		write  bool
		key    string
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	var plan []req
	total := sc.RPS * sc.Seconds
	keyN := 0
	var lastKeys []string
	for i := 0; i < total; i++ {
		r := req{at: time.Duration(float64(i) / float64(sc.RPS) * float64(time.Second))}
		r.client = fmt.Sprintf("tenant-%02d", rng.IntN(sc.Clients))
		r.write = rng.Float64() < sc.WriteRatio
		if r.write {
			if len(lastKeys) > 0 && rng.Float64() < sc.DupRatio {
				r.key = lastKeys[rng.IntN(len(lastKeys))]
			} else {
				keyN++
				r.key = fmt.Sprintf("k-%d", keyN)
				lastKeys = append(lastKeys, r.key)
				if len(lastKeys) > 64 {
					lastKeys = lastKeys[1:]
				}
			}
		}
		plan = append(plan, r)
	}
	if sc.HotRPS > 0 {
		for i := 0; i < sc.HotRPS*sc.Seconds; i++ {
			plan = append(plan, req{at: time.Duration(float64(i) / float64(sc.HotRPS) * float64(time.Second)), client: "tenant-hot", hot: true})
		}
		sort.SliceStable(plan, func(i, j int) bool { return plan[i].at < plan[j].at })
	}

	// ids creados (para que las lecturas peguen a tickets reales del mismo tenant)
	var idsMu sync.Mutex
	ids := map[string][]string{}
	keyClient := map[string]string{}
	for _, r := range plan {
		if r.write {
			if _, ok := keyClient[r.key]; !ok {
				keyClient[r.key] = r.client
			}
		}
	}

	var mu sync.Mutex
	codes := map[string]int{}
	var all, normal, writes, reads []float64
	var hotN, hot429, normN, norm429, skipped int
	okKeys := map[string]bool{}
	var terr atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 2048)
	start := time.Now()
	for _, r := range plan {
		if d := time.Until(start.Add(r.at)); d > 0 {
			time.Sleep(d)
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(r req) {
			defer func() { <-sem; wg.Done() }()
			intended := start.Add(r.at)
			var hr *http.Request
			client := r.client
			if r.write {
				client = keyClient[r.key]
				body, _ := json.Marshal(map[string]string{"customer_id": "cust-" + client, "subject": "No llegó mi pedido " + r.key, "body": "Pagué y no llegó.", "channel": "chat"})
				hr, _ = http.NewRequest("POST", base+"/v1/tickets", bytes.NewReader(body))
				hr.Header.Set("Idempotency-Key", r.key)
				hr.Header.Set("Content-Type", "application/json")
			} else {
				idsMu.Lock()
				lst := ids[client]
				var id string
				if len(lst) > 0 {
					id = lst[len(lst)-1-int(r.at)%min(len(lst), 50)]
				}
				idsMu.Unlock()
				if id == "" {
					hr, _ = http.NewRequest("GET", base+"/v1/tickets?limit=20", nil)
				} else {
					hr, _ = http.NewRequest("GET", base+"/v1/tickets/"+id, nil)
				}
			}
			hr.Header.Set("X-Client-ID", client)
			res, err := cl.Do(hr)
			el := float64(time.Since(intended).Microseconds()) / 1000
			if err != nil {
				terr.Add(1)
				return
			}
			b, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if r.write && res.StatusCode == 201 {
				var t struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(b, &t) == nil && t.ID != "" {
					idsMu.Lock()
					ids[client] = append(ids[client], t.ID)
					idsMu.Unlock()
				}
			}
			mu.Lock()
			defer mu.Unlock()
			codes[fmt.Sprint(res.StatusCode)]++
			if r.write && (res.StatusCode == 201 || res.StatusCode == 200) {
				okKeys[r.key] = true
			}
			if r.at < warmup {
				skipped++
				return
			}
			all = append(all, el)
			if r.write {
				writes = append(writes, el)
			} else if !r.hot {
				reads = append(reads, el)
			}
			if r.hot {
				hotN++
				if res.StatusCode == 429 {
					hot429++
				}
			} else {
				normal = append(normal, el)
				normN++
				if res.StatusCode == 429 {
					norm429++
				}
			}
		}(r)
	}
	wg.Wait()
	elapsed := time.Since(start)
	// espera a que el trabajo asíncrono termine (o 10 s)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := a.Queue.Stats()
		if a.Store.OutboxLen() == 0 && s.Ready+s.Delayed+s.InFlight == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fams, _ := a.Metrics.Registry.Gather()
	hit := counterSum(fams, "cache_requests_total", "result", "hit")
	tot := counterSum(fams, "cache_requests_total", "", "")
	st := a.Queue.Stats()
	nc, nf := a.Notifier.Calls()
	drain := map[string]any{
		"outbox_pending": a.Store.OutboxLen(), "queue_ready": st.Ready, "queue_delayed": st.Delayed, "dlq": st.DLQ,
		"notified": a.Notified(), "notifier_calls": nc, "notifier_failed": nf,
		"breaker_notifier": a.NotifyCB.State().String(), "breaker_triage": a.TriageCB.State().String(),
		"triage_agent":          counterSum(fams, "triage_results_total", "source", "agent"),
		"triage_fallback_total": counterSum(fams, "triage_results_total", "", "") - counterSum(fams, "triage_results_total", "source", "agent"),
	}
	res := result{Scenario: sc, WarmupSkipped: skipped, Sent: len(plan), Achieved: float64(len(plan)) / elapsed.Seconds(), ByCode: codes, TransportErr: int(terr.Load()),
		LatencyMS: summarize(all), NormalMS: summarize(normal), WriteMS: summarize(writes), ReadMS: summarize(reads), TriageSources: triageSources(fams), UniqueKeys: len(okKeys), TicketsStored: a.Store.Count(), Drain: drain}
	res.Achieved = float64(int(res.Achieved*10+0.5)) / 10
	if tot > 0 {
		res.CacheHitRatio = float64(int(hit/tot*1000+0.5)) / 1000
	}
	if hotN > 0 {
		res.HotRejectRatio = float64(int(float64(hot429)/float64(hotN)*1000+0.5)) / 1000
	}
	if normN > 0 {
		res.NormalReject = float64(int(float64(norm429)/float64(normN)*10000+0.5)) / 10000
	}
	cancel()
	<-served
	return res, nil
}

func main() {
	out := flag.String("out", "results", "directorio de salida")
	secs := flag.Int("seconds", 8, "duración de cada escenario")
	seed := flag.Uint64("seed", 20261112, "semilla del plan de carga")
	reps := flag.Int("reps", 3, "repeticiones por escenario")
	flag.Parse()
	scs := []scenario{
		{Name: "baseline", Description: "Mezcla 30 % POST / 70 % GET, 50 tenants, 10 % de POST repetidos con la misma Idempotency-Key, sin fallas.",
			RPS: 2000, Seconds: *secs, WriteRatio: .3, DupRatio: .1, Clients: 50, RatePerSec: 1000, RateBurst: 2000},
		{Name: "downstream_chaos", Description: "Igual que baseline pero el notificador y el agente de IA fallan el 50 % de las veces.",
			RPS: 2000, Seconds: *secs, WriteRatio: .3, DupRatio: .1, Clients: 50, ChaosErr: .5, RatePerSec: 1000, RateBurst: 2000},
		{Name: "noisy_neighbor", Description: "Baseline a 1000 rps más un tenant abusivo a 1500 rps con límite de 200 rps por tenant.",
			RPS: 1000, Seconds: *secs, WriteRatio: .3, DupRatio: .1, Clients: 50, HotRPS: 1500, RatePerSec: 200, RateBurst: 400},
	}
	var results []result
	for _, sc := range scs {
		for rep := 1; rep <= *reps; rep++ {
			fmt.Fprintf(os.Stderr, "running %s rep %d...\n", sc.Name, rep)
			r, err := run(sc, *seed)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			r.Rep = rep
			results = append(results, r)
		}
	}
	env := map[string]any{"goos": runtime.GOOS, "goarch": runtime.GOARCH, "go": runtime.Version(), "num_cpu": runtime.NumCPU(), "seed": *seed,
		"note": "generador y servidor comparten la misma máquina y CPU (loopback)"}
	_ = os.MkdirAll(*out, 0o755)
	raw, _ := json.MarshalIndent(map[string]any{"environment": env, "results": results}, "", "  ")
	if err := os.WriteFile(*out+"/loadtest.json", append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Resultados de la prueba de carga\n\nGenerado por `go run ./cmd/loadtest` (semilla %d, %d s por escenario, %d repeticiones). El primer segundo de cada corrida es calentamiento y no entra en los percentiles.\n\n", *seed, *secs, *reps)
	fmt.Fprintf(&b, "Entorno: %s/%s, %s, %d CPU. Generador y servidor en la misma máquina (loopback): los números son relativos a este hardware.\n\n", runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.NumCPU())
	b.WriteString("| Escenario | Rep | Enviadas | rps logrado | p50 ms | p99 ms | p99.9 ms | max ms | códigos | cache hit | claves únicas OK / tickets |\n|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s | %d | %d | %.1f | %.3f | %.3f | %.3f | %.3f | %v | %.3f | %d / %d |\n", r.Scenario.Name, r.Rep, r.Sent, r.Achieved, r.LatencyMS.P50, r.LatencyMS.P99, r.LatencyMS.P999, r.LatencyMS.Max, r.ByCode, r.CacheHitRatio, r.UniqueKeys, r.TicketsStored)
	}
	b.WriteString("\n## Detalle por escenario\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "### %s (repetición %d)\n\n%s\n\n- Clientes normales: p50 %.3f ms, p99 %.3f ms, 429 = %.4f\n", r.Scenario.Name, r.Rep, r.Scenario.Description, r.NormalMS.P50, r.NormalMS.P99, r.NormalReject)
		fmt.Fprintf(&b, "- POST: p50 %.3f ms, p99 %.3f ms. GET: p50 %.3f ms, p99 %.3f ms\n", r.WriteMS.P50, r.WriteMS.P99, r.ReadMS.P50, r.ReadMS.P99)
		fmt.Fprintf(&b, "- Triage por fuente: %v\n", r.TriageSources)
		if r.Scenario.HotRPS > 0 {
			fmt.Fprintf(&b, "- Tenant abusivo: %.3f de sus requests recibieron 429\n", r.HotRejectRatio)
		}
		dk := make([]string, 0, len(r.Drain))
		for k := range r.Drain {
			dk = append(dk, k)
		}
		sort.Strings(dk)
		b.WriteString("- Después de drenar:")
		for _, k := range dk {
			fmt.Fprintf(&b, " %s=%v;", k, r.Drain[k])
		}
		fmt.Fprintf(&b, "\n- Errores de transporte: %d\n\n", r.TransportErr)
	}
	if err := os.WriteFile(*out+"/LOADTEST.md", []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(b.String())
}
