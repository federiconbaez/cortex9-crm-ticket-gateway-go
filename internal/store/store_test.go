package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/ticket"
)

func mk(id, client string) ticket.Ticket {
	return ticket.Ticket{ID: id, ClientID: client, Status: ticket.StatusOpen, Version: 1}
}

func TestTxAtomicity(t *testing.T) {
	s := New()
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx *Tx) error {
		tx.InsertTicket(mk("a", "c"))
		tx.AppendOutbox(OutboxEvent{ID: "e1", Type: "ticket.created", AggregateID: "a"})
		return nil
	})
	if err != nil || s.Count() != 1 || s.OutboxLen() != 1 {
		t.Fatalf("commit failed: %v count=%d outbox=%d", err, s.Count(), s.OutboxLen())
	}
	// fn devuelve error: rollback total.
	boom := errors.New("boom")
	err = s.WithTx(ctx, func(tx *Tx) error {
		tx.InsertTicket(mk("b", "c"))
		tx.AppendOutbox(OutboxEvent{ID: "e2"})
		return boom
	})
	if !errors.Is(err, boom) || s.Count() != 1 || s.OutboxLen() != 1 {
		t.Fatalf("rollback failed")
	}
	// duplicado en commit: nada se aplica, tampoco el evento.
	err = s.WithTx(ctx, func(tx *Tx) error {
		tx.InsertTicket(mk("c", "c"))
		tx.InsertTicket(mk("a", "c"))
		tx.AppendOutbox(OutboxEvent{ID: "e3"})
		return nil
	})
	if !errors.Is(err, ErrDuplicate) || s.Count() != 1 || s.OutboxLen() != 1 {
		t.Fatalf("partial commit on duplicate: %v", err)
	}
}

func TestOptimisticVersion(t *testing.T) {
	s := New()
	ctx := context.Background()
	_ = s.WithTx(ctx, func(tx *Tx) error { tx.InsertTicket(mk("a", "c")); return nil })
	err := s.WithTx(ctx, func(tx *Tx) error {
		cur, err := tx.GetForUpdate("a")
		if err != nil {
			return err
		}
		exp := cur.Version
		_ = cur.Transition(ticket.StatusInProgress, time.Now())
		tx.UpdateTicket(cur, exp)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "a")
	if got.Version != 2 || got.Status != ticket.StatusInProgress {
		t.Fatalf("got %+v", got)
	}
	stale := got
	stale.Status = ticket.StatusResolved
	err = s.WithTx(ctx, func(tx *Tx) error { tx.UpdateTicket(stale, 1); return nil })
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	err = s.WithTx(ctx, func(tx *Tx) error { tx.UpdateTicket(mk("zz", "c"), 1); return nil })
	if !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	err = s.WithTx(ctx, func(tx *Tx) error { _, err := tx.GetForUpdate("nope"); return err })
	if !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestKeysetPaginationStableUnderInserts(t *testing.T) {
	s := New()
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		_ = s.WithTx(ctx, func(tx *Tx) error { tx.InsertTicket(mk(fmt.Sprintf("t%02d", i), "c1")); return nil })
		_ = s.WithTx(ctx, func(tx *Tx) error { tx.InsertTicket(mk(fmt.Sprintf("o%02d", i), "c2")); return nil })
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		p, err := s.List(ctx, ListQuery{ClientID: "c1", Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range p.Items {
			if it.ClientID != "c1" || seen[it.ID] {
				t.Fatalf("leak or dup: %+v", it)
			}
			seen[it.ID] = true
		}
		if pages == 1 { // inserción concurrente entre páginas
			_ = s.WithTx(ctx, func(tx *Tx) error { tx.InsertTicket(mk("t99", "c1")); return nil })
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(seen) != 26 || pages != 3 {
		t.Fatalf("seen=%d pages=%d", len(seen), pages)
	}
	// filtro por estado
	p, _ := s.List(ctx, ListQuery{ClientID: "c1", Status: ticket.StatusClosed})
	if len(p.Items) != 0 || p.NextCursor != "" {
		t.Fatal("status filter")
	}
	p, _ = s.List(ctx, ListQuery{ClientID: "c1", Limit: 1000})
	if len(p.Items) != 20 {
		t.Fatalf("limit clamp: %d", len(p.Items))
	}
}

func TestCursor(t *testing.T) {
	for _, bad := range []string{"!!!", "dGVzdA", EncodeCursor(5)[:2]} {
		if _, err := DecodeCursor(bad); !errors.Is(err, ErrBadCursor) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	if n, err := DecodeCursor(EncodeCursor(42)); err != nil || n != 42 {
		t.Fatal(n, err)
	}
	if _, err := New().List(context.Background(), ListQuery{Cursor: "x"}); !errors.Is(err, ErrBadCursor) {
		t.Fatal(err)
	}
	bad := "djE6YWJj" // "v1:abc"
	if _, err := DecodeCursor(bad); !errors.Is(err, ErrBadCursor) {
		t.Fatal(err)
	}
}

func TestOutboxLifecycle(t *testing.T) {
	now := time.Unix(1000, 0)
	s := New(WithClock(func() time.Time { return now }))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = s.WithTx(ctx, func(tx *Tx) error { tx.AppendOutbox(OutboxEvent{ID: fmt.Sprint(i)}); return nil })
	}
	now = now.Add(3 * time.Second)
	if s.OldestPendingAge() != 3*time.Second {
		t.Fatal(s.OldestPendingAge())
	}
	got := s.PendingOutbox(3)
	if len(got) != 3 || got[0].ID != "0" || got[2].ID != "2" {
		t.Fatalf("%+v", got)
	}
	if n := s.MarkPublished([]string{"0", "2", "zz"}); n != 2 {
		t.Fatal(n)
	}
	if n := s.MarkPublished([]string{"0"}); n != 0 {
		t.Fatal("not idempotent")
	}
	got = s.PendingOutbox(10)
	if len(got) != 3 || got[0].ID != "1" || got[1].ID != "3" {
		t.Fatalf("%+v", got)
	}
	s.MarkPublished(nil)
	s.MarkPublished([]string{"1", "3", "4"})
	if s.OutboxLen() != 0 || s.OldestPendingAge() != 0 {
		t.Fatal("not empty")
	}
}

func TestSimulatedPoolHonoursContext(t *testing.T) {
	s := New(WithSimulatedDB(1, 50*time.Millisecond))
	_ = s.WithTx(context.Background(), func(tx *Tx) error { tx.InsertTicket(mk("a", "c")); return nil })
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = s.Get(context.Background(), "a") }()
	time.Sleep(10 * time.Millisecond)
	if s.PoolInUse() != 1 {
		t.Fatal("pool should be busy")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := s.Get(ctx, "a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	wg.Wait()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	if _, err := s.List(ctx2, ListQuery{ClientID: "c"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("latency must respect ctx: %v", err)
	}
	if New().PoolInUse() != 0 {
		t.Fatal()
	}
	cctx, c := context.WithCancel(context.Background())
	c()
	if err := s.WithTx(cctx, func(*Tx) error { return nil }); err == nil {
		t.Fatal("cancelled ctx")
	}
}

func TestConcurrentTx(t *testing.T) {
	s := New()
	ctx := context.Background()
	_ = s.WithTx(ctx, func(tx *Tx) error { tx.InsertTicket(mk("a", "c")); return nil })
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, conflicts := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cur, _ := s.Get(ctx, "a")
			exp := cur.Version
			cur.Version++
			err := s.WithTx(ctx, func(tx *Tx) error { tx.UpdateTicket(cur, exp); return nil })
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else if errors.Is(err, ErrVersionConflict) {
				conflicts++
			}
		}()
	}
	wg.Wait()
	got, _ := s.Get(ctx, "a")
	if int(got.Version-1) != ok || ok+conflicts != 50 {
		t.Fatalf("lost update: version=%d ok=%d conflicts=%d", got.Version, ok, conflicts)
	}
}
