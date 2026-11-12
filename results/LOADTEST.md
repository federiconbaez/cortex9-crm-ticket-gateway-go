# Resultados de la prueba de carga

Generado por `go run ./cmd/loadtest` (semilla 20261112, 8 s por escenario, 3 repeticiones). El primer segundo de cada corrida es calentamiento y no entra en los percentiles.

Entorno: linux/amd64, go1.27.3, 8 CPU. Generador y servidor en la misma máquina (loopback): los números son relativos a este hardware.

| Escenario | Rep | Enviadas | rps logrado | p50 ms | p99 ms | p99.9 ms | max ms | códigos | cache hit | claves únicas OK / tickets |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 1 | 16000 | 1999.6 | 1.620 | 4.198 | 4.602 | 5.537 | map[200:11213 201:4777 409:10] | 0.657 | 4306 / 4306 |
| baseline | 2 | 16000 | 1999.4 | 1.577 | 4.229 | 4.648 | 5.386 | map[200:11213 201:4778 409:9] | 0.658 | 4306 / 4306 |
| baseline | 3 | 16000 | 1999.6 | 1.596 | 4.224 | 4.580 | 4.833 | map[200:11213 201:4779 409:8] | 0.658 | 4306 / 4306 |
| downstream_chaos | 1 | 16000 | 1999.3 | 1.333 | 4.296 | 4.785 | 5.982 | map[200:11213 201:4779 409:8] | 0.693 | 4306 / 4306 |
| downstream_chaos | 2 | 16000 | 1999.3 | 1.294 | 4.427 | 6.672 | 12.675 | map[200:11213 201:4777 409:10] | 0.702 | 4306 / 4306 |
| downstream_chaos | 3 | 16000 | 1999.5 | 1.318 | 4.282 | 4.788 | 5.718 | map[200:11213 201:4778 409:9] | 0.694 | 4306 / 4306 |
| noisy_neighbor | 1 | 20000 | 2499.5 | 0.878 | 4.108 | 4.517 | 6.641 | map[200:7594 201:2404 409:1 429:10001] | 0.659 | 2166 / 2166 |
| noisy_neighbor | 2 | 20000 | 2499.7 | 0.908 | 171.029 | 234.582 | 254.238 | map[200:7594 201:2404 409:1 429:10001] | 0.660 | 2166 / 2166 |
| noisy_neighbor | 3 | 20000 | 2499.7 | 0.858 | 4.082 | 4.447 | 4.720 | map[200:7594 201:2402 409:3 429:10001] | 0.661 | 2166 / 2166 |

## Detalle por escenario

### baseline (repetición 1)

Mezcla 30 % POST / 70 % GET, 50 tenants, 10 % de POST repetidos con la misma Idempotency-Key, sin fallas.

- Clientes normales: p50 1.620 ms, p99 4.198 ms, 429 = 0.0000
- POST: p50 3.190 ms, p99 4.359 ms. GET: p50 0.943 ms, p99 4.099 ms
- Triage por fuente: map[agent:3806 fallback_low_confidence:500]
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=4306; notifier_calls=4306; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=3806; triage_fallback_total=500;
- Errores de transporte: 0

### baseline (repetición 2)

Mezcla 30 % POST / 70 % GET, 50 tenants, 10 % de POST repetidos con la misma Idempotency-Key, sin fallas.

- Clientes normales: p50 1.577 ms, p99 4.229 ms, 429 = 0.0000
- POST: p50 3.218 ms, p99 4.358 ms. GET: p50 0.956 ms, p99 4.129 ms
- Triage por fuente: map[agent:3806 fallback_low_confidence:500]
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=4306; notifier_calls=4306; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=3806; triage_fallback_total=500;
- Errores de transporte: 0

### baseline (repetición 3)

Mezcla 30 % POST / 70 % GET, 50 tenants, 10 % de POST repetidos con la misma Idempotency-Key, sin fallas.

- Clientes normales: p50 1.596 ms, p99 4.224 ms, 429 = 0.0000
- POST: p50 3.217 ms, p99 4.324 ms. GET: p50 0.939 ms, p99 4.127 ms
- Triage por fuente: map[agent:3806 fallback_low_confidence:500]
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=4306; notifier_calls=4306; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=3806; triage_fallback_total=500;
- Errores de transporte: 0

### downstream_chaos (repetición 1)

Igual que baseline pero el notificador y el agente de IA fallan el 50 % de las veces.

- Clientes normales: p50 1.333 ms, p99 4.296 ms, 429 = 0.0000
- POST: p50 3.165 ms, p99 4.420 ms. GET: p50 0.928 ms, p99 4.165 ms
- Triage por fuente: map[agent:138 fallback_agent_error:154 fallback_breaker_open:3996 fallback_low_confidence:18]
- Después de drenar: breaker_notifier=open; breaker_triage=open; dlq=4268; notified=38; notifier_calls=85; notifier_failed=47; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=138; triage_fallback_total=4168;
- Errores de transporte: 0

### downstream_chaos (repetición 2)

Igual que baseline pero el notificador y el agente de IA fallan el 50 % de las veces.

- Clientes normales: p50 1.294 ms, p99 4.427 ms, 429 = 0.0000
- POST: p50 3.202 ms, p99 4.684 ms. GET: p50 0.912 ms, p99 4.266 ms
- Triage por fuente: map[agent:34 fallback_agent_error:38 fallback_breaker_open:4231 fallback_low_confidence:3]
- Después de drenar: breaker_notifier=open; breaker_triage=half_open; dlq=4268; notified=38; notifier_calls=85; notifier_failed=47; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=34; triage_fallback_total=4272;
- Errores de transporte: 0

### downstream_chaos (repetición 3)

Igual que baseline pero el notificador y el agente de IA fallan el 50 % de las veces.

- Clientes normales: p50 1.318 ms, p99 4.282 ms, 429 = 0.0000
- POST: p50 3.213 ms, p99 4.415 ms. GET: p50 0.930 ms, p99 4.180 ms
- Triage por fuente: map[agent:109 fallback_agent_error:118 fallback_breaker_open:4067 fallback_low_confidence:12]
- Después de drenar: breaker_notifier=open; breaker_triage=open; dlq=4267; notified=39; notifier_calls=87; notifier_failed=48; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=109; triage_fallback_total=4197;
- Errores de transporte: 0

### noisy_neighbor (repetición 1)

Baseline a 1000 rps más un tenant abusivo a 1500 rps con límite de 200 rps por tenant.

- Clientes normales: p50 2.205 ms, p99 4.209 ms, 429 = 0.0000
- POST: p50 3.207 ms, p99 4.322 ms. GET: p50 0.939 ms, p99 4.129 ms
- Triage por fuente: map[agent:1907 fallback_low_confidence:259]
- Tenant abusivo: 0.867 de sus requests recibieron 429
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=2166; notifier_calls=2166; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=1907; triage_fallback_total=259;
- Errores de transporte: 0

### noisy_neighbor (repetición 2)

Baseline a 1000 rps más un tenant abusivo a 1500 rps con límite de 200 rps por tenant.

- Clientes normales: p50 2.540 ms, p99 173.209 ms, 429 = 0.0000
- POST: p50 3.265 ms, p99 169.685 ms. GET: p50 0.999 ms, p99 174.460 ms
- Triage por fuente: map[agent:1907 fallback_low_confidence:259]
- Tenant abusivo: 0.867 de sus requests recibieron 429
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=2166; notifier_calls=2166; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=1907; triage_fallback_total=259;
- Errores de transporte: 0

### noisy_neighbor (repetición 3)

Baseline a 1000 rps más un tenant abusivo a 1500 rps con límite de 200 rps por tenant.

- Clientes normales: p50 1.724 ms, p99 4.183 ms, 429 = 0.0000
- POST: p50 3.203 ms, p99 4.301 ms. GET: p50 0.929 ms, p99 4.114 ms
- Triage por fuente: map[agent:1907 fallback_low_confidence:259]
- Tenant abusivo: 0.867 de sus requests recibieron 429
- Después de drenar: breaker_notifier=closed; breaker_triage=closed; dlq=0; notified=2166; notifier_calls=2166; notifier_failed=0; outbox_pending=0; queue_delayed=0; queue_ready=0; triage_agent=1907; triage_fallback_total=259;
- Errores de transporte: 0

