# ADR-0002: Idempotency-Key con lease y token de dueño

**Estado:** aceptada · **Ref:** CX9-6171

## Contexto

Los clientes reintentan POST y PATCH ante timeouts. Sin protección, un reintento crea tickets duplicados o aplica dos veces una transición de estado.

## Decisión

- `Idempotency-Key` **obligatoria** en POST y PATCH (400 si falta).
- Huella = SHA-256(método, ruta, cuerpo). Misma clave con otra huella: 422.
- La clave queda acotada por cliente (`X-Client-ID` + clave): dos tenants pueden usar la misma clave sin chocar.
- Se guardan las respuestas 2xx y 4xx durante 24 h. **No** se guardan (*Abort*) las 5xx, los timeouts ni las respuestas que dependen del estado del momento (409, 412 y 429), para que el cliente pueda reintentar con la misma clave.
- Si la misma clave llega mientras la primera sigue en curso: **409 inmediato** por defecto (configurable para esperar con `IDEMPOTENCY_WAIT`).
- Entradas en curso con lease de 30 s y token de dueño.

## Alternativas consideradas

| Alternativa | Por qué no |
|---|---|
| Deduplicar por contenido, sin clave | Dos reclamos legítimos idénticos del mismo cliente se fusionarían |
| Restricción única en la base (`customer_id + subject`) | Mismo problema y no sirve para PATCH |
| Esperar siempre a la primera en lugar de 409 | Ocupa conexiones y goroutines durante ráfagas de reintentos; 409 + reintento del cliente es más barato. Queda como opción. |
| Clave opcional | Los clientes que no la manden quedan desprotegidos sin saberlo |

## Consecuencias

- Los clientes deben generar una clave por intención (UUID v4 o una clave de negocio, como `pedido-123-reclamo`).
- Medido: 9 de 9 corridas sin duplicados (claves aceptadas = tickets guardados). Hubo entre 1 y 11 respuestas 409 por corrida, por claves repetidas en paralelo.
- `TestPropertyMatchesModel` encontró que sin token de dueño un `Complete` tardío podía pisar a un nuevo dueño después del vencimiento del lease. El token lo resuelve.
- Con varias réplicas, el store pasa a Redis o a una tabla; ver las limitaciones en el README.
