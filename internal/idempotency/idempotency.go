// Package idempotency guarda el resultado de requests no idempotentes
// (POST/PATCH) bajo una Idempotency-Key elegida por el cliente, con TTL.
//
// Semántica (alineada con draft-ietf-httpapi-idempotency-key-header):
//   - Primera vez que llega una key: Started. El handler corre y al terminar
//     se llama Complete con la respuesta (o Abort si fue un 5xx/timeout, para
//     que el cliente pueda reintentar con la misma key).
//   - Misma key y mismo fingerprint, ya completada: Replay (misma respuesta,
//     byte a byte, sin re-ejecutar efectos).
//   - Misma key con otro fingerprint (otro body/método/ruta): Mismatch -> 422.
//   - Misma key mientras la primera sigue en vuelo: InFlight -> 409, o el
//     llamador puede esperar el canal Ready hasta su deadline.
//
// Las entradas en vuelo tienen un "lease": si el proceso que la tomó nunca
// llama Complete/Abort (bug, panic no recuperado), la key se libera sola.
package idempotency

import (
	"crypto/sha256"
	"hash/maphash"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Outcome de Begin.
type Outcome int

const (
	Started Outcome = iota
	Replay
	Mismatch
	InFlight
)

func (o Outcome) String() string {
	return [...]string{"started", "replay", "mismatch", "in_flight"}[o]
}

// Fingerprint identifica el contenido de la request.
type Fingerprint [32]byte

// FingerprintOf calcula SHA-256(method \x00 path \x00 body).
func FingerprintOf(method, path string, body []byte) Fingerprint {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	var fp Fingerprint
	copy(fp[:], h.Sum(nil))
	return fp
}

// Response almacenada para replays.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

type entry struct {
	token   uint64
	fp      Fingerprint
	done    bool
	resp    Response
	expires time.Time
	ready   chan struct{}
}

const nShards = 32

type shard struct {
	mu sync.Mutex
	m  map[string]*entry
}

// Store en memoria. En producción: Redis (SET NX PX) o una tabla con PK.
type Store struct {
	ttl    time.Duration
	lease  time.Duration
	shards [nShards]shard
	seed   maphash.Seed
	now    func() time.Time
	gen    atomic.Uint64
}

// Token identifica a quien tomó la key en Begin. Complete y Abort lo exigen:
// así, si el lease de A vence y B toma la misma key, un Complete tardío de A
// no puede pisar ni cerrar la entrada de B (bug encontrado por el test
// property-based, ver docs/conceptos/idempotencia.md).
type Token uint64

// Begin result.
type Result struct {
	Outcome  Outcome
	Response *Response       // si Replay
	Ready    <-chan struct{} // si InFlight
	Token    Token           // si Started
}

// New crea un Store. ttl: cuánto se recuerda una respuesta; lease: máximo
// que una key puede quedar "en vuelo".
func New(ttl, lease time.Duration) *Store {
	s := &Store{ttl: ttl, lease: lease, seed: maphash.MakeSeed(), now: time.Now}
	for i := range s.shards {
		s.shards[i].m = map[string]*entry{}
	}
	return s
}

func (s *Store) shard(k string) *shard { return &s.shards[maphash.String(s.seed, k)%nShards] }

// Begin intenta tomar key. Si Outcome es Replay devuelve la respuesta. Si es
// InFlight devuelve un canal que se cierra cuando la primera termina.
func (s *Store) Begin(key string, fp Fingerprint) Result {
	sh := s.shard(key)
	now := s.now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.m[key]
	if ok && !now.Before(e.expires) {
		if !e.done {
			close(e.ready) // lease vencido: liberamos a los que esperan
		}
		delete(sh.m, key)
		ok = false
	}
	if !ok {
		tok := s.gen.Add(1)
		sh.m[key] = &entry{token: tok, fp: fp, expires: now.Add(s.lease), ready: make(chan struct{})}
		return Result{Outcome: Started, Token: Token(tok)}
	}
	if e.fp != fp {
		return Result{Outcome: Mismatch}
	}
	if !e.done {
		return Result{Outcome: InFlight, Ready: e.ready}
	}
	r := e.resp
	return Result{Outcome: Replay, Response: &r}
}

// owned devuelve la entrada si tok sigue siendo su dueño y el lease no venció.
func (s *Store) owned(sh *shard, key string, tok Token) *entry {
	e, ok := sh.m[key]
	if !ok || e.token != uint64(tok) || e.done || !s.now().Before(e.expires) {
		return nil
	}
	return e
}

// Complete guarda la respuesta de key si tok sigue siendo el dueño.
// Devuelve false si el lease se perdió.
func (s *Store) Complete(key string, tok Token, resp Response) bool {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := s.owned(sh, key, tok)
	if e == nil {
		return false
	}
	e.done = true
	e.resp = resp
	e.expires = s.now().Add(s.ttl)
	close(e.ready)
	return true
}

// Abort libera key sin guardar respuesta (errores reintentables).
func (s *Store) Abort(key string, tok Token) bool {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := s.owned(sh, key, tok)
	if e == nil {
		return false
	}
	delete(sh.m, key)
	close(e.ready)
	return true
}

// Sweep elimina entradas vencidas. Devuelve cuántas.
func (s *Store) Sweep() int {
	now := s.now()
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			if !now.Before(e.expires) {
				if !e.done {
					close(e.ready)
				}
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// Len devuelve la cantidad de keys.
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += len(s.shards[i].m)
		s.shards[i].mu.Unlock()
	}
	return n
}
