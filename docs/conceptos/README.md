# Conceptos

Cada archivo explica un concepto desde cero, con un diagrama y con el lugar exacto del código donde está implementado.

| Concepto | Archivo | Código |
|---|---|---|
| Idempotencia e Idempotency-Key | [idempotencia.md](idempotencia.md) | `internal/idempotency`, `internal/api/middleware.go` |
| Transactional outbox y cola | [outbox-y-cola.md](outbox-y-cola.md) | `internal/store`, `internal/outbox`, `internal/queue` |
| Rate limit y load shedding | [proteccion-de-carga.md](proteccion-de-carga.md) | `internal/ratelimit`, `internal/limiter` |
| Caché, singleflight y XFetch | [cache.md](cache.md) | `internal/cache`, `internal/singleflight` |
| Circuit breaker, timeout y fallback | [resiliencia.md](resiliencia.md) | `internal/breaker`, `internal/triage` |
| Observabilidad, RED y SLO | [observabilidad-y-slo.md](observabilidad-y-slo.md) | `internal/observability`, `deploy/` |
