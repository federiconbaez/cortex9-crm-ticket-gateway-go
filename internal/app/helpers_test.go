package app

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/downstream"
)

func testConfig() Config {
	c := Defaults()
	c.NotifyLatency = 0
	c.TriageLatency = 0
	c.BaseBackoff = time.Millisecond
	c.MaxBackoff = 5 * time.Millisecond
	c.OutboxInterval = 5 * time.Millisecond
	c.ShutdownTimeout = 5 * time.Second
	return c
}

type harness struct {
	t   *testing.T
	a   *App
	srv *httptest.Server
}

func newHarness(t *testing.T, mut func(*Config)) *harness {
	t.Helper()
	c := testConfig()
	if mut != nil {
		mut(&c)
	}
	a, err := New(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.StartBackground()
	srv := httptest.NewServer(a.API)
	h := &harness{t: t, a: a, srv: srv}
	t.Cleanup(func() {
		srv.Close()
		a.Shutdown(contextWithTimeout(5 * time.Second))
	})
	return h
}

type resp struct {
	Code   int
	Header http.Header
	Body   []byte
}

func (r resp) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
}

func (h *harness) do(method, path, client string, body any, hdr map[string]string) resp {
	h.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = bytes.NewBufferString(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rd)
	if client != "" {
		req.Header.Set("X-Client-ID", client)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := h.srv.Client().Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{Code: res.StatusCode, Header: res.Header, Body: b}
}

func newTicketBody(subject string) map[string]any {
	return map[string]any{"customer_id": "cust-1", "subject": subject, "channel": "chat"}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func quantile(ds []time.Duration, q float64) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if len(s) == 0 {
		return 0
	}
	idx := int(q*float64(len(s))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

func zeroChaos() downstream.Chaos { return downstream.Chaos{} }
