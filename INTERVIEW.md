# Preguntas de entrevista sobre cortex9-crm-ticket-gateway-go

Cómo usar este archivo: leé la pregunta, respondé en voz alta sin mirar (2 minutos como máximo) y después compará con la respuesta modelo. Las respuestas usan los números medidos en `results/`. **Siempre aclarar que es un proyecto personal de estudio**, en memoria y en un solo proceso.

## Pitch de 30 segundos

"Armé un gateway de tickets de atención al cliente en Go, pensado para alto tráfico. La request solo valida, guarda el ticket y su evento en una transacción y responde. La clasificación con IA y las notificaciones corren después en una cola con reintentos, breakers y fallback a reglas. Lo medí con una prueba de carga de lazo abierto: a 2000 requests por segundo el p99 fue de unos 4 ms, y cuando hice fallar al agente de IA y al notificador el 50 % de las veces, la API ni se enteró: el mismo p99 y cero errores 5xx. Tiene 97 % de cobertura, métricas Prometheus, un tablero de Grafana y alertas por SLO."

---

## A. Diseño y arquitectura

### 1. ¿Por qué no clasificás el ticket con la IA dentro del POST?

**Respuesta modelo:** porque la disponibilidad y la latencia de la API pasarían a ser las del modelo. Un LLM tarda de cientos de milisegundos a segundos y falla. El POST pone una prioridad inicial por reglas (instantánea), guarda y responde; el agente corre después en un worker. Lo medí: con el agente fallando el 50 %, el p99 de la API pasó de 4.2 a 4.3 ms.
**Trade-off:** durante unos milisegundos el ticket tiene la prioridad de las reglas y después puede cambiar. Si el cliente fijó una prioridad, se respeta.

### 2. ¿Qué es el transactional outbox y qué problema resuelve?

**Respuesta modelo:** el problema de la doble escritura. Guardar el ticket y publicar el evento son dos sistemas sin una transacción común; un crash en el medio deja inconsistencia. Escribo el evento en una tabla outbox en la misma transacción que el ticket y un relay lo publica después.
**Trade-off:** la entrega es al menos una vez, así que el consumidor tiene que ser idempotente. Y hay que vigilar la antigüedad de la outbox: tengo una alerta a los 30 s.

### 3. ¿"Exactamente una vez" existe?

**Respuesta modelo:** como entrega en red, no. Lo que se logra es entrega al menos una vez más un consumidor idempotente, que da un efecto "exactamente una vez". En mi caso la cola deduplica por ID de evento y el triage no reclasifica un ticket ya clasificado.

### 4. ¿Cómo escalarías esto a varias réplicas?

**Respuesta modelo:** el servicio HTTP es casi sin estado, salvo tres cosas en memoria: rate limit, idempotencia y caché. La idempotencia pasa a Redis con `SET NX PX` o a una tabla con PK en la misma transacción. El rate limit, a Redis con un script atómico o a un límite local por réplica (límite global dividido por la cantidad de réplicas), aceptando imprecisión. La caché puede seguir local con TTL corto, o pasar a Redis compartido. La outbox se vuelve una tabla real y el relay corre con un *lock* o con particiones para no publicar en paralelo el mismo evento; o directamente se usa CDC con Debezium.
**Trade-off:** el rate limit centralizado es exacto pero suma un salto de red por request; el local es gratis pero aproximado.

### 5. ¿Por qué Go?

**Respuesta modelo:** goroutines baratas para miles de conexiones concurrentes, un binario estático fácil de desplegar, una biblioteca estándar que trae HTTP de producción, `context` para propagar deadlines y cancelación, y un detector de carreras integrado que en este proyecto encontró un bug real. Mi experiencia viene de C, Rust, .NET y Java, y Go me resultó natural: el modelo de memoria se parece al de C y los conceptos de concurrencia al de Java.

### 6. ¿Qué cambiarías si fuera producción en Mercado Libre?

**Respuesta modelo:** base real (MySQL o Postgres) con la outbox como tabla; la cola interna de la empresa o Kafka en lugar de mi cola en memoria; autenticación con OAuth en lugar de `X-Client-ID`; idempotencia y rate limit en un store compartido; despliegue con readiness y preStop alineados al apagado ordenado que ya implementé; y una prueba de carga en una máquina dedicada, separada del servidor.

## B. Alto tráfico y protección de carga

### 7. Diferencia entre rate limiting y load shedding

**Respuesta modelo:** el rate limit protege de un cliente abusivo: token bucket por cliente, con 429. El load shedding protege al proceso cuando todos juntos superan la capacidad: limita la concurrencia, con 503. Medí el primero: un tenant a 1500 rps con un límite de 200 recibió 429 en el 86.7 % de sus requests y los demás en el 0 %.

### 8. Explicá el token bucket

**Respuesta modelo:** un balde de capacidad `Burst` que se rellena a `Rate` por segundo; cada request gasta un token. Permite ráfagas y limita el promedio. El relleno es perezoso: se calcula con el tiempo transcurrido al llegar la request, sin goroutines por cliente. El mapa está dividido en shards para reducir la contención de locks y un barrido borra los baldes inactivos.
**Trade-off frente a la ventana fija:** la ventana fija permite el doble del límite en el borde de dos ventanas.

### 9. ¿Por qué limitar la concurrencia y no las requests por segundo?

**Respuesta modelo:** por la ley de Little: en vuelo = tasa × latencia. Si la base se pone lenta, con la misma tasa crece la concurrencia, todo hace cola en el pool y todo se vuelve lento. Con AIMD el límite sube de a poco mientras la latencia está bajo el objetivo y baja un 10 % cuando se pasa. El exceso se rechaza al instante. Es mejor rechazar rápido el 10 % que responder lento al 100 %.

### 10. ¿Cómo diseñarías una API para 100 000 requests por segundo?

**Respuesta modelo:** primero preguntar la mezcla de lecturas y escrituras, el tamaño de los payloads, la latencia objetivo y la consistencia que se necesita. Después: servicio sin estado detrás de un balanceador con escalado horizontal; caché para lecturas (local más Redis) con protección contra estampida; escrituras con trabajo mínimo en la request y el resto asíncrono por outbox y cola; base particionada por tenant o por ID; rate limit por cliente y shedding por instancia; timeouts y breakers hacia cada dependencia; observabilidad RED con SLO. Con un número de referencia: si una instancia aguanta X rps con un p99 dentro del objetivo, hacen falta 100k/X instancias más un margen del 30 al 50 % para picos y caídas de zona. Ese X lo saco de una prueba de carga, no de la intuición.

### 11. ¿Qué es la omisión coordinada y cómo la evitaste?

**Respuesta modelo:** cuando el generador espera una respuesta antes de mandar la siguiente, si el servidor se traba el generador también se frena y las demoras no aparecen en los percentiles. Usé lazo abierto: requests programadas a tasa fija, con la latencia medida desde la hora programada. Además descarto el primer segundo como calentamiento y publico las tres repeticiones, incluida una con un p99 de 171 ms que atribuyo a ruido de la máquina compartida.

## C. Datos, consistencia y caché

### 12. ¿Qué es una estampida de caché y cómo la evitás?

**Respuesta modelo:** vence una clave popular y miles de requests van a la base a la vez. Singleflight: una sola carga por clave; los demás esperan ese resultado. Y XFetch: cada lectura decide con una probabilidad que crece cerca del vencimiento si refresca en segundo plano antes de que venza. Los dos tienen tests.
**Trade-off:** con TTL de 5 s e invalidación local, otra réplica puede servir datos de hasta 5 s de antigüedad. Para el estado de un ticket es aceptable.

### 13. ¿Cómo evitás que dos agentes pisen el mismo ticket?

**Respuesta modelo:** concurrencia optimista: cada ticket tiene una versión expuesta como `ETag`. El PATCH manda `If-Match: "v3"`; si la versión actual es otra, 412. No se toman locks largos.
**Trade-off:** en mi implementación `If-Match` es opcional para no romper clientes viejos. En un sistema nuevo lo haría obligatorio (428 si falta).

### 14. ¿Por qué paginación keyset y no OFFSET?

**Respuesta modelo:** `OFFSET 100000` obliga a la base a recorrer y descartar 100 000 filas, y si se insertan filas mientras se pagina, se saltan o se repiten elementos. Con keyset el cursor dice "después de este ID": usa el índice, cuesta lo mismo en cualquier página y es estable ante inserciones. Hay un test que inserta mientras pagina.

### 15. ¿Cómo garantizás que un reintento no cree un ticket duplicado?

**Respuesta modelo:** `Idempotency-Key` obligatoria y acotada por cliente. Guardo la huella del cuerpo y la respuesta: el reintento recibe la misma respuesta byte a byte. Misma clave con otro cuerpo da 422; en curso, 409. Las 5xx no se guardan, para poder reintentar. Medido: en 9 corridas, claves aceptadas = tickets guardados.

## D. Resiliencia

### 16. Explicá el circuit breaker y cómo elegiste los parámetros

**Respuesta modelo:** tres estados. Cerrado: pasa todo y se cuentan los errores en una ventana de 10 s con 10 buckets. Si hay 20 o más llamadas y falla el 50 % o más, se abre: falla al instante durante 2 s. Después pasa a semiabierto con 3 pruebas. El mínimo de 20 evita abrir por 1 de 2. Los valores son puntos de partida; en producción los ajustaría mirando la tasa de error normal de cada dependencia. Medido: con el notificador fallando el 50 %, hubo 85 llamadas en lugar de 4306.

### 17. ¿Qué pasa con los mensajes cuando el breaker está abierto? (pregunta trampa)

**Respuesta modelo:** es justamente un problema que encontré midiendo. Cada intento falla al instante, los 6 reintentos se agotan rápido y casi todo terminó en la DLQ: entre 4267 y 4268 de 4306. No se pierde nada porque hay redrive, pero es ruido. La solución es no contar el rechazo por breaker abierto como intento, reprogramar para cuando el breaker pase a semiabierto o pausar ese consumidor. Lo dejé documentado en un ADR en lugar de cambiarlo sin medir.

### 18. ¿Por qué *full jitter* en los reintentos?

**Respuesta modelo:** si mil mensajes fallan juntos y todos esperan exactamente 100, 200 y 400 ms, reintentan sincronizados y vuelven a tumbar a la dependencia. Con *full jitter* cada uno espera un tiempo al azar entre 0 y el tope exponencial, y los reintentos se reparten.

### 19. ¿Cómo hace el apagado ordenado Kubernetes y cómo lo implementaste?

**Respuesta modelo:** Kubernetes manda SIGTERM y, en paralelo, saca el pod de los endpoints, pero eso tarda unos segundos en propagarse. Por eso primero pongo `/readyz` en 503 y espero un `PreStopDelay`; después `http.Server.Shutdown` espera las requests en curso; el relay publica lo que quedó en la outbox; la cola drena; se vacían las trazas y al final se apaga el puerto de métricas. Cada etapa usa una fracción del tiempo restante. Encontré un bug: un segundo `Shutdown` entraba en pánico por cerrar un canal dos veces. Lo resolví con `sync.Once`.

## E. Observabilidad

### 20. ¿Qué métricas mirás primero en un incidente?

**Respuesta modelo:** RED por ruta: tasa, errores y p99. Después, la saturación de cada componente: profundidad de la cola, antigüedad de la outbox, estado de los breakers, límite AIMD y pool de base. Si los errores suben y un breaker está abierto, el problema está aguas abajo; si sube la latencia y el pool está lleno, es la base. Después voy a las trazas de una request lenta concreta.

### 21. ¿Por qué p99 y no el promedio?

**Respuesta modelo:** el promedio esconde la cola. Con millones de requests, el 1 % más lento son miles de clientes. Además, una página que hace 20 llamadas tiene alrededor de un 18 % de probabilidad de sufrir al menos un p99 (1 − 0.99^20).

### 22. ¿Qué es un SLO y cómo alertás?

**Respuesta modelo:** un objetivo sobre un indicador; por ejemplo, 99.9 % de requests sin 5xx en 30 días. El 0.1 % restante es el presupuesto de errores, unos 43 minutos por mes. Alerto por burn rate en dos ventanas: si consumo 14.4 veces más rápido de lo sostenible durante 1 h y también en los últimos 5 min, se llama a la guardia. La ventana corta hace que la alerta se apague rápido cuando el problema se resuelve.

### 23. ¿Usaste Grafana o Datadog?

**Respuesta modelo (honesta):** en este proyecto armé el tablero de Grafana como código y un test verifica que todas las métricas que usa existen. No lo levanté en un Grafana real porque no tenía Docker en el entorno. Datadog no lo usé en producción; como instrumenté con Prometheus y OpenTelemetry, que son estándares abiertos, el agente de Datadog puede leer el mismo `/metrics` y recibir las trazas por OTLP. Lo que sí traigo de mi experiencia es [completar con tu caso real de monitoreo en Telecom, Naranja X o GCBA].

## F. Calidad y proceso

### 24. ¿Cómo testeás algo concurrente?

**Respuesta modelo:** tres herramientas. Siempre `-race` en CI: encontró una carrera real en la cola, donde copiaba un mensaje después de soltar el lock. Tests de invariantes con muchas goroutines: el rate limit nunca deja pasar más que el burst y el AIMD nunca supera su límite. Y un test basado en propiedades que compara la idempotencia contra un modelo simple con miles de secuencias aleatorias: encontró el bug del lease.

### 25. ¿97 % de cobertura no es demasiado? ¿Es una buena métrica?

**Respuesta modelo:** la cobertura dice qué código **no** se ejecutó nunca, no que el ejecutado sea correcto. Lo valioso son los tests de comportamiento: caos, apagado y propiedades. El número alto aquí sale de que los componentes son chicos y puros. En un equipo no pondría un umbral rígido del 90 %; pediría tests para cada bug corregido y para cada camino de error importante.

### 26. ¿Qué harías distinto si empezaras de nuevo?

**Respuesta modelo:** empezaría por la prueba de carga y el tablero antes que por las optimizaciones, para que cada decisión tenga un número antes y después. Y separaría el "breaker abierto" del contador de reintentos desde el diseño de la cola.

## Preguntas para hacerle al entrevistador

- ¿Cómo manejan hoy la idempotencia y los reintentos entre servicios en el área de atención al cliente?
- ¿Qué parte del flujo de soporte ya usa agentes de IA, y cómo miden su calidad?
- ¿Qué SLO tiene el equipo y cómo se decide entre deuda técnica y funcionalidades?
