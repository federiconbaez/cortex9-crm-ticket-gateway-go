# ADR-0001: Transactional outbox para publicar eventos

**Estado:** aceptada · **Ref:** CX9-6171

## Contexto

Al crear un ticket hay que guardarlo y disparar trabajo asíncrono (triage con IA y notificación). Base y cola son sistemas distintos, sin una transacción común. Un crash entre las dos escrituras deja el sistema inconsistente: un ticket que nadie clasifica, o un evento de un ticket inexistente.

## Decisión

Escribir el evento en una tabla `outbox` dentro de la misma transacción que el ticket. Un relay lee los pendientes en orden, los publica y los marca. El handler llama a `Kick()` para que el relay no espere su intervalo (50 ms), y así la latencia típica entre commit y publicación queda baja.

## Alternativas consideradas

| Alternativa | Por qué no |
|---|---|
| Publicar en la cola desde el handler, después del commit | Un crash entre commit y publicación pierde el evento |
| Transacción distribuida (2PC/XA) | Lenta, acopla la disponibilidad de la base y la cola, y pocos brokers modernos la soportan |
| *Change Data Capture* (Debezium leyendo el binlog) | Excelente en producción, pero agrega infraestructura. Es el paso natural cuando la outbox crece. |
| Escribir solo en la cola y que el consumidor guarde (*event sourcing*) | Cambia todo el modelo de datos; excesivo para este caso |

## Consecuencias

- Entrega **al menos una vez**: el consumidor tiene que ser idempotente (deduplicación por ID de evento en la cola y triage que no reclasifica).
- La outbox crece y hay que limpiarla (aquí se borran los eventos al marcarlos; en SQL, un job que borre los publicados de más de N días).
- Nueva métrica crítica: antigüedad del evento pendiente más viejo (`outbox_oldest_pending_age_seconds`), con alerta a los 30 s.
- Medido: en la prueba de carga, después de drenar, la outbox quedó siempre en 0 pendientes.
