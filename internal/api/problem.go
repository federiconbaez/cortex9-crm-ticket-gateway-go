package api

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Problem es un error RFC 9457 (application/problem+json).
type Problem struct {
	Type    string   `json:"type"`
	Title   string   `json:"title"`
	Status  int      `json:"status"`
	Detail  string   `json:"detail,omitempty"`
	Allowed []string `json:"allowed,omitempty"`
}

func writeProblem(w http.ResponseWriter, status int, typ, detail string) {
	writeProblemFull(w, Problem{Type: "https://errors.cortex9.dev/" + typ, Title: http.StatusText(status), Status: status, Detail: detail})
}

func writeProblemFull(w http.ResponseWriter, p Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Del("Content-Length")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "internal", "encode")
		return
	}
	b = append(b, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
