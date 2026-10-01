# Informe - TP Coordinación: Sistemas Distribuidos


| Alumno              | Padron | email            |
|---------------------|--------|------------------|
| Franco Daniel Capra | 99642  | fcapra@fi.uba.ar |


Todo nombre de cola, exchange, routing key, host y cantidad de réplicas sale de variables de entorno 
(`*_PREFIX`, `*_AMOUNT`, `*_QUEUE`, `MOM_*`), de modo que el sistema funciona con nombres arbitrarios.

## Protocolo de comunicación

### Interno

Una interfaz sellada `Message` con cuatro implementaciones concretas, serializadas como en JSON posicional con el tipo primero:

| tipo | struct   | Contenido                                                    |
|------|----------|--------------------------------------------------------------|
| `D`  | `Data`   | clientId, fruta, cantidad                                    |
| `E`  | `EOF`    | clientId, total de registros del cliente                     |
| `T`  | `Top`    | clientId, lista de pares (fruta, cantidad)                   |
| `R`  | `Report` | clientId, replicaId, conteo absoluto de registros procesados |

## Coordinación

### Separación de flujos por cliente

Cada conexión TCP recibe en el gateway un `MessageHandler` con un `clientId` único (contador atómico). Todos los 
mensajes internos llevan el `clientId`

Las réplicas de Sum consumen en competencia la cola de entrada, así que el EOF de un cliente llega a una sola. 

1. La réplica que recibe el EOF lo **reenvía al exchange de control** `<SUM_PREFIX>_control`.
2. Al recibir el aviso, **cada** réplica abre una ronda para el cliente (guarda el `total`) y **reporta de inmediato** 
3. su conteo de registros procesados (`R`).
3. Cada réplica lleva el **máximo reportado por cada par** y, cuando la suma alcanza el `total`, envia.
4. Durante la ronda, cada registro procesado actualiza el conteo y republica (`R`).

Se eligió esta barrera porque no depende del prefetch ni del orden de despacho.

### Partición en Aggregation

Sum publica cada fruta por `outputExchanges[hashFruit(fruta) % M]`. Cada fruta 
llega a **exactamente un** aggregator y una sola vez, eliminando el broadcast redundante.

### Barreras en cadena

- **Aggregation** cuenta `SUM_AMOUNT` EOF por cliente. 
Como cada réplica de Sum publica sus datos y luego su `E` **en orden sobre su propia conexión**, 
al consumir el EOF Nº `SUM_AMOUNT` ya se consumieron todos los datos de todas las réplicas. Ahí calcula el top parcial.
- **Join** junta `AGGREGATION_AMOUNT` tops `T` por cliente, los fusiona y publica el top final.

