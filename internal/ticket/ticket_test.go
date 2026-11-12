package ticket

import (
	"errors"
	"testing"
	"time"
)

func TestTransitionsTable(t *testing.T) {
	all := []Status{StatusOpen, StatusInProgress, StatusWaitingCustomer, StatusResolved, StatusClosed}
	want := map[[2]Status]bool{
		{StatusOpen, StatusInProgress}: true, {StatusOpen, StatusClosed}: true,
		{StatusInProgress, StatusWaitingCustomer}: true, {StatusInProgress, StatusResolved}: true, {StatusInProgress, StatusOpen}: true,
		{StatusWaitingCustomer, StatusInProgress}: true, {StatusWaitingCustomer, StatusResolved}: true, {StatusWaitingCustomer, StatusClosed}: true,
		{StatusResolved, StatusClosed}: true, {StatusResolved, StatusOpen}: true,
	}
	for _, from := range all {
		for _, to := range all {
			if got := CanTransition(from, to); got != want[[2]Status{from, to}] {
				t.Errorf("CanTransition(%s,%s)=%v", from, to, got)
			}
		}
	}
}

func TestTransitionBumpsVersion(t *testing.T) {
	tk := &Ticket{Status: StatusOpen, Version: 1}
	now := time.Unix(100, 0)
	if err := tk.Transition(StatusInProgress, now); err != nil {
		t.Fatal(err)
	}
	if tk.Version != 2 || !tk.UpdatedAt.Equal(now) {
		t.Fatalf("version/updated not bumped: %+v", tk)
	}
	if err := tk.Transition(StatusClosed, now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("want invalid transition, got %v", err)
	}
	if err := tk.Transition("bogus", now); !errors.Is(err, ErrValidation) {
		t.Fatalf("want validation, got %v", err)
	}
	closed := &Ticket{Status: StatusClosed}
	if len(AllowedFrom(StatusClosed)) != 0 || closed.Transition(StatusOpen, now) == nil {
		t.Fatal("closed must be terminal")
	}
}

func TestValidate(t *testing.T) {
	ok := CreateInput{CustomerID: " c1 ", Subject: " hola ", Channel: "EMAIL"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	if ok.CustomerID != "c1" || ok.Channel != "email" {
		t.Fatalf("not normalized: %+v", ok)
	}
	bad := []CreateInput{
		{Subject: "x", Channel: "email"},
		{CustomerID: "c", Channel: "email"},
		{CustomerID: "c", Subject: "x", Channel: "fax"},
		{CustomerID: "c", Subject: "x", Channel: "chat", Priority: "P9"},
		{CustomerID: "c", Subject: "x", Channel: "chat", Body: string(make([]byte, 10001))},
	}
	for i, in := range bad {
		if err := in.Validate(); !errors.Is(err, ErrValidation) {
			t.Errorf("case %d: want validation error, got %v", i, err)
		}
	}
}

func TestPriority(t *testing.T) {
	if P1.Rank() >= P2.Rank() || P3.Rank() >= P4.Rank() || Priority("x").Rank() != 3 {
		t.Fatal("rank order")
	}
	if P1.SLA() >= P2.SLA() || P2.SLA() >= P3.SLA() || P3.SLA() >= P4.SLA() {
		t.Fatal("sla order")
	}
	if ValidPriority("P5") || !ValidPriority(P4) {
		t.Fatal("valid priority")
	}
}
