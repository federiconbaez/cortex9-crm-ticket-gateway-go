// Package ticket contiene el dominio: el ticket de atención al cliente, su
// máquina de estados y la prioridad derivada del SLA.
package ticket

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Status es el estado de un ticket.
type Status string

const (
	StatusOpen            Status = "open"
	StatusInProgress      Status = "in_progress"
	StatusWaitingCustomer Status = "waiting_customer"
	StatusResolved        Status = "resolved"
	StatusClosed          Status = "closed"
)

// Priority es la prioridad operativa (P1 es la más urgente).
type Priority string

const (
	P1 Priority = "P1"
	P2 Priority = "P2"
	P3 Priority = "P3"
	P4 Priority = "P4"
)

// Rank devuelve 0 para P1 ... 3 para P4; sirve para ordenar colas.
func (p Priority) Rank() int {
	switch p {
	case P1:
		return 0
	case P2:
		return 1
	case P3:
		return 2
	default:
		return 3
	}
}

// SLA de primera respuesta por prioridad.
func (p Priority) SLA() time.Duration {
	switch p {
	case P1:
		return 15 * time.Minute
	case P2:
		return time.Hour
	case P3:
		return 8 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// ValidPriority indica si p es una prioridad conocida.
func ValidPriority(p Priority) bool {
	switch p {
	case P1, P2, P3, P4:
		return true
	}
	return false
}

// transitions es la tabla de la máquina de estados. Todo lo que no está acá
// es inválido. "closed" es terminal; "resolved" se puede reabrir.
var transitions = map[Status][]Status{
	StatusOpen:            {StatusInProgress, StatusClosed},
	StatusInProgress:      {StatusWaitingCustomer, StatusResolved, StatusOpen},
	StatusWaitingCustomer: {StatusInProgress, StatusResolved, StatusClosed},
	StatusResolved:        {StatusClosed, StatusOpen},
	StatusClosed:          {},
}

// ErrInvalidTransition se devuelve cuando la transición no está permitida.
var ErrInvalidTransition = errors.New("invalid status transition")

// ErrValidation agrupa errores de validación de entrada.
var ErrValidation = errors.New("validation error")

// ValidStatus indica si s es un estado conocido.
func ValidStatus(s Status) bool {
	_, ok := transitions[s]
	return ok
}

// CanTransition indica si from -> to es válido.
func CanTransition(from, to Status) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// AllowedFrom devuelve los destinos válidos desde from (copia).
func AllowedFrom(from Status) []Status {
	return append([]Status(nil), transitions[from]...)
}

// Ticket es la entidad persistida.
type Ticket struct {
	ID         string   `json:"id"`
	ClientID   string   `json:"client_id"`
	CustomerID string   `json:"customer_id"`
	Subject    string   `json:"subject"`
	Body       string   `json:"body"`
	Channel    string   `json:"channel"`
	Status     Status   `json:"status"`
	Priority   Priority `json:"priority"`
	// PrioritySource: client | rules | agent | fallback_*.
	PrioritySource string    `json:"priority_source"`
	Category       string    `json:"category,omitempty"`
	TriagedBy      string    `json:"triaged_by,omitempty"`
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	DueAt          time.Time `json:"due_at"`
}

// Transition aplica la transición validando la máquina de estados y la
// versión esperada (control de concurrencia optimista).
func (t *Ticket) Transition(to Status, now time.Time) error {
	if !ValidStatus(to) {
		return fmt.Errorf("%w: unknown status %q", ErrValidation, to)
	}
	if !CanTransition(t.Status, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, to)
	}
	t.Status = to
	t.Version++
	t.UpdatedAt = now
	return nil
}

// CreateInput es lo que llega por la API.
type CreateInput struct {
	CustomerID string   `json:"customer_id"`
	Subject    string   `json:"subject"`
	Body       string   `json:"body"`
	Channel    string   `json:"channel"`
	Priority   Priority `json:"priority,omitempty"`
}

var validChannels = map[string]bool{"email": true, "chat": true, "phone": true, "web": true, "app": true}

// Validate valida y normaliza la entrada.
func (in *CreateInput) Validate() error {
	in.CustomerID = strings.TrimSpace(in.CustomerID)
	in.Subject = strings.TrimSpace(in.Subject)
	in.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	var problems []string
	if in.CustomerID == "" {
		problems = append(problems, "customer_id is required")
	}
	if in.Subject == "" || len(in.Subject) > 200 {
		problems = append(problems, "subject must have 1..200 bytes")
	}
	if len(in.Body) > 10_000 {
		problems = append(problems, "body must have at most 10000 bytes")
	}
	if !validChannels[in.Channel] {
		problems = append(problems, "channel must be one of email|chat|phone|web|app")
	}
	if in.Priority != "" && !ValidPriority(in.Priority) {
		problems = append(problems, "priority must be P1..P4")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrValidation, strings.Join(problems, "; "))
	}
	return nil
}
