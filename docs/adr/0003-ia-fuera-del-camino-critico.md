# ADR-0003: El agente de IA fuera del camino crítico de la request

**Estado:** aceptada · **Ref:** CX9-6171

## Contexto

Clasificar un ticket con un modelo de IA mejora el ruteo, pero un LLM tiene latencias de cientos de milisegundos a segundos, falla, tiene límites de tasa y a veces devuelve basura. Si el POST esperara al modelo, la disponibilidad de la API sería, como mucho, la del proveedor de IA.

## Decisión

1. El POST asigna una prioridad inicial por **reglas** (instantáneas) y responde.
2. El triage con IA corre en un worker, a partir del evento de la outbox.
3. Alrededor del agente: breaker propio, timeout de 500 ms, validación de la salida, umbral de confianza de 0.6 y fallback a reglas en todos los casos.
4. Se registra la fuente de cada clasificación (`triage_results_total{source}`) para medir cuánto aporta la IA y cuánto el fallback.

## Alternativas consideradas

| Alternativa | Por qué no |
|---|---|
| Clasificar síncronamente en el POST | La latencia y los errores del modelo pasan directo al cliente |
| Sin fallback: dejar el ticket "sin clasificar" | Tickets urgentes podrían quedar sin prioridad durante un incidente del proveedor |
| Reintentar el modelo varias veces | Multiplica el costo y la carga justo cuando el proveedor está mal; el breaker es mejor |

## Consecuencias

- Durante unos milisegundos el ticket tiene la prioridad de las reglas; después puede cambiar. Si el cliente fijó la prioridad, se respeta siempre.
- Medido: con el 50 % de fallas en el agente y el notificador, el p99 de la API fue de 4.28 a 4.43 ms, contra 4.20 a 4.23 ms con todo sano. El 100 % de los tickets quedó clasificado.

## Problema detectado y siguiente paso

Con el breaker del notificador abierto, cada intento falla al instante y los 6 intentos se agotan rápido (en la prueba, con un backoff máximo de 200 ms). Resultado: entre 4267 y 4268 de 4306 eventos terminaron en la DLQ. No se pierden (hay *redrive*), pero es ruido operativo. Siguiente paso: **no contar como intento un rechazo por breaker abierto** y reprogramar el mensaje para cuando el breaker pase a semiabierto, o pausar el consumo de ese tipo de mensaje. Elegí documentarlo en lugar de cambiarlo a último momento sin medirlo.
