# ADR-0004: Prueba de carga de lazo abierto dentro del proceso

**Estado:** aceptada · **Ref:** CX9-6171

## Contexto

Quería números reproducibles que cualquiera pueda regenerar con un comando, en CI y sin infraestructura. Las herramientas de lazo cerrado (cada usuario virtual espera la respuesta antes de mandar la siguiente) subestiman la latencia cuando el servidor se traba: el generador baja la tasa sin querer y las demoras desaparecen de los percentiles (*coordinated omission*, descrito por Gil Tene).

## Decisión

- `cmd/loadtest` levanta el gateway real (los mismos `app.New` y `Serve` que en producción) en `127.0.0.1` con un puerto efímero.
- Plan de requests determinista (semilla fija) a tasa constante; la latencia se mide desde la hora **programada**.
- Se descarta el primer segundo como calentamiento, contado aparte.
- 3 escenarios × 3 repeticiones, y se publican todas las repeticiones, no solo la mejor.
- El reporte guarda el entorno (CPU, Go, GOOS) junto con los números.
- En CI corre una versión de humo de 2 s que verifica que funcione, sin comparar tiempos (las máquinas de CI son ruidosas).

## Alternativas consideradas

| Alternativa | Por qué no (por ahora) |
|---|---|
| k6, vegeta o wrk2 contra un binario | Excelentes y de lazo abierto (vegeta y wrk2), pero requieren instalar algo más y separar procesos; quería un solo comando |
| Medir en una máquina dedicada | No disponible; queda explicitado en las limitaciones |

## Consecuencias

- Generador y servidor compiten por la misma CPU: los números absolutos son pesimistas y sirven para **comparar escenarios**.
- La repetición 2 de `noisy_neighbor` dio un p99 de 171 ms, contra alrededor de 4.1 ms en las otras: ruido de la máquina compartida. Publicar todas las repeticiones lo deja a la vista.
