# AgroCenter Digital - ms-inventario

Microservicio responsable del catalogo, las existencias y la trazabilidad de
inventario de AgroCenter Digital. Forma parte del MVP Cloud Native descrito para
la plataforma y mantiene el limite **Database-per-Service**: solo accede a
`db_inventario`.

## Responsabilidad

`ms-inventario` administra:

- productos e insumos agricolas;
- stock actual y umbral minimo;
- validacion de disponibilidad;
- entradas informadas por `ms-compras`;
- salidas solicitadas por `ms-ventas`;
- productos con stock bajo;
- historial auditable de movimientos;
- idempotencia de operaciones por producto, tipo y referencia.

No contiene logica de compras, ventas, autenticacion de usuarios, carrito,
checkout ni frontend. Tampoco consulta `db_compras` o `db_ventas`.

## Arquitectura

```text
Controller -> Service (@Transactional) -> Repository (JPA) -> db_inventario
```

Las modificaciones de stock se realizan dentro del servicio. Para evitar que
dos ventas simultaneas produzcan stock negativo, el producto se obtiene con un
bloqueo pesimista de escritura. La entidad mantiene ademas `@Version` como
proteccion ante actualizaciones concurrentes del catalogo.

La combinacion `(producto_id, tipo_movimiento, referencia)` es unica. Un reintento
de red con la misma referencia devuelve el resultado original con
`"duplicada": true` y no vuelve a modificar el stock.

## Tecnologias

- Java 21.
- Spring Boot 4.1.0.
- Spring Web MVC.
- Spring Data JPA / Hibernate.
- Spring Security OAuth2 Resource Server.
- PostgreSQL; Amazon RDS en produccion.
- Bean Validation.
- Spring Boot Actuator.
- OpenAPI / Swagger UI con springdoc.
- Docker.
- JUnit, Mockito, MockMvc y H2 para pruebas.

## Requisitos

- JDK 21.
- Docker, opcional.
- PostgreSQL 14 o superior para ejecucion local sin Docker.
- Una base de datos llamada `db_inventario`.
- Datos de AWS Cognito para probar JWT reales.

## Variables de entorno

El archivo `.env.example` contiene una plantilla sin secretos.

| Variable | Descripcion | Valor local por defecto |
|---|---|---|
| `SERVER_PORT` | Puerto HTTP | `8081` |
| `DB_URL` | URL JDBC completa | Construida con host, puerto y nombre |
| `DB_USERNAME` | Usuario PostgreSQL | Fallback a `DB_USER` y luego `postgres` |
| `DB_PASSWORD` | Contrasena PostgreSQL | Obligatoria |
| `DB_HOST` | Host legado soportado | `localhost` |
| `DB_PORT` | Puerto legado soportado | `5432` |
| `DB_NAME` | Base de datos legada soportada | `db_inventario` |
| `DB_USER` | Usuario legado soportado | `postgres` |
| `COGNITO_ISSUER_URI` | Issuer exacto del User Pool | Sin valor real por defecto |
| `COGNITO_JWK_SET_URI` | JWKS del User Pool | Sin valor real por defecto |
| `COGNITO_AUDIENCE` | Audience esperada | `agrocenter-api` |
| `SPRING_PROFILES_ACTIVE` | Perfil activo (`dev`, `local` o `prod`) | Compose usa `dev` |
| `DEV_JWT_SECRET` | Clave HMAC local de al menos 32 caracteres | Obligatoria en `dev` |
| `DEV_JWT_ISSUER` | Issuer de los JWT locales | `http://localhost:8081/dev-issuer` |
| `DEV_JWT_TTL_SECONDS` | Duracion del token local, entre 60 y 86400 segundos | `3600` |
| `SWAGGER_ENABLED` | Habilita OpenAPI/Swagger | `true` |
| `JPA_SHOW_SQL` | Muestra SQL | `false` |

En ambientes reales se deben suministrar `DB_PASSWORD`, issuer, JWKS y audience
desde el gestor de configuracion o secretos del entorno. No deben confirmarse en
Git.

## Configuracion de PostgreSQL

Ejemplo con `psql`:

```sql
CREATE DATABASE db_inventario;
```

Ejemplo de variables para PowerShell:

```powershell
$env:DB_URL = "jdbc:postgresql://localhost:5432/db_inventario"
$env:DB_USERNAME = "postgres"
$env:DB_PASSWORD = "tu-contrasena-local"
$env:COGNITO_ISSUER_URI = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_POOL"
$env:COGNITO_JWK_SET_URI = "$env:COGNITO_ISSUER_URI/.well-known/jwks.json"
$env:COGNITO_AUDIENCE = "agrocenter-api"
```

El perfil predeterminado mantiene `ddl-auto=update` para no romper la ejecucion
local existente. El perfil `prod` utiliza `ddl-auto=validate`; el esquema debe
estar provisionado antes del despliegue productivo.

## Ejecucion local

```powershell
.\mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=local
```

El servicio queda disponible en `http://localhost:8081`.

## Ejecucion con Docker

El archivo `compose.yaml` levanta el microservicio y una instancia dedicada de
PostgreSQL 16. La base queda persistida en un volumen Docker y se publica en el
puerto `5434` para no interferir con instalaciones locales que usen `5432` o
`5433`.

Crear la configuracion local:

```powershell
Copy-Item .env.example .env
```

Antes de continuar, editar `.env` y reemplazar `POSTGRES_PASSWORD` y
`DEV_JWT_SECRET`. Para trabajar con el perfil `dev` no es necesario configurar
Cognito. El archivo `.env` esta excluido de Git.

Construir y levantar el stack:

```powershell
docker compose up --build -d
docker compose ps
```

Comprobar el microservicio:

```powershell
curl.exe http://localhost:8081/actuator/health
```

Consultar logs y detener los contenedores:

```powershell
docker compose logs -f ms-inventario
docker compose down
```

`docker compose down` conserva los datos. Para eliminar tambien la base de datos
local se debe ejecutar expresamente `docker compose down --volumes`.

Si solo se necesita construir la imagen:

```bash
docker build -t agrocenter/ms-inventario:local .
```

Para ejecutar unicamente la imagen contra un PostgreSQL instalado en el host:

```bash
docker run --rm -p 8081:8081 \
  -e DB_URL=jdbc:postgresql://host.docker.internal:5432/db_inventario \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=tu-contrasena-local \
  -e COGNITO_ISSUER_URI=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_POOL \
  -e COGNITO_JWK_SET_URI=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_POOL/.well-known/jwks.json \
  -e COGNITO_AUDIENCE=agrocenter-api \
  agrocenter/ms-inventario:local
```

La imagen se construye en dos etapas, ejecuta Java 21 con un usuario no root e
incluye un health check sobre `/actuator/health`. En Compose, el microservicio
espera a que PostgreSQL este saludable, se ejecuta con filesystem de solo lectura,
sin capabilities Linux y con un limite porcentual de memoria para la JVM.

## Pruebas con Postman sin Cognito

El perfil `dev` habilita un emisor JWT local para pruebas manuales. El endpoint
no se registra en `local` ni en `prod`, y la configuracion impide activarlo si el
perfil `prod` tambien esta presente.

Con el stack Docker saludable, generar un token de administrador:

```http
POST http://localhost:8081/api/dev/token
Content-Type: application/json

{
  "usuario": "postman-admin",
  "rol": "ADMIN"
}
```

Para simular un cliente, usar `"rol": "CLIENTE"`. La respuesta contiene
`accessToken`, `tokenType`, `expiresIn`, `expiresAt` y `rol`.

En Postman, copiar `accessToken` en **Authorization > Bearer Token** y consumir
los endpoints bajo `http://localhost:8081/api/inventario`. Ejemplo:

```http
GET http://localhost:8081/api/inventario/productos
Authorization: Bearer <accessToken>
```

Los tokens locales usan HS256, issuer y audience controlados por el perfil
`dev`, además de los mismos grupos `ADMIN`/`CLIENTE` que utiliza la conversion de
claims de Cognito. Esta capacidad es solo para desarrollo; el despliegue real
debe usar el perfil `prod` y JWT emitidos por Cognito.

## Endpoints

Todos los endpoints de negocio requieren JWT. Swagger y health se habilitan para
desarrollo.

| Metodo | Ruta | Acceso | Descripcion |
|---|---|---|---|
| `POST` | `/api/dev/token` | Publico solo en perfil `dev` | Generar JWT local para Postman |
| `GET` | `/api/inventario/productos` | CLIENTE, ADMIN, scope read/write | Catalogo con filtros `categoria`, `nombre`, `activo` |
| `GET` | `/api/inventario/productos/{id}` | CLIENTE, ADMIN, scope read/write | Producto por ID |
| `GET` | `/api/inventario/productos/sku/{sku}` | CLIENTE, ADMIN, scope read/write | Producto por SKU |
| `POST` | `/api/inventario/productos` | ADMIN | Crear producto |
| `PUT` | `/api/inventario/productos/{id}` | ADMIN | Actualizar datos del producto |
| `PATCH` | `/api/inventario/productos/{id}/estado` | ADMIN | Activar o desactivar |
| `GET` | `/api/inventario/productos/{id}/stock` | CLIENTE, ADMIN, scope read/write | Consultar existencias |
| `POST` | `/api/inventario/stock/validar` | CLIENTE, ADMIN, scope read/write | Validar disponibilidad |
| `POST` | `/api/inventario/stock/entrada` | ADMIN o scope write | Incrementar stock desde `ms-compras` |
| `POST` | `/api/inventario/stock/salida` | CLIENTE, ADMIN o scope write | Descontar stock desde `ms-ventas` |
| `GET` | `/api/inventario/movimientos` | ADMIN | Historial paginado |
| `GET` | `/api/inventario/productos/{id}/movimientos` | ADMIN | Historial paginado por producto |
| `GET` | `/actuator/health` | Publico dentro de la red | Estado basico sin detalles |
| `GET` | `/swagger-ui.html` | Desarrollo | Documentacion interactiva |
| `GET` | `/v3/api-docs` | Desarrollo | Contrato OpenAPI JSON |

Para historial se aceptan `pagina` (desde `0`) y `tamanio` (entre `1` y `100`).

## Ejemplos de requests y responses

Crear un producto:

```http
POST /api/inventario/productos
Authorization: Bearer <jwt-admin>
Content-Type: application/json

{
  "sku": "SEM-001",
  "nombre": "Semilla de Maiz",
  "descripcion": "Saco de semillas de 20 kg",
  "categoria": "Semillas",
  "precioVenta": 24990.00,
  "stockMinimo": 20
}
```

El producto se crea activo y con stock `0`. Todo aumento posterior debe ingresar
por una operacion de stock para conservar trazabilidad.

Validar disponibilidad:

```http
POST /api/inventario/stock/validar
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "productoId": 1,
  "cantidad": 5
}
```

```json
{
  "productoId": 1,
  "disponible": true,
  "stockActual": 120,
  "cantidadSolicitada": 5
}
```

Registrar una entrada desde `ms-compras`:

```http
POST /api/inventario/stock/entrada
Authorization: Bearer <jwt-admin-o-token-interno>
Content-Type: application/json

{
  "productoId": 1,
  "cantidad": 50,
  "referencia": "COMPRA-123"
}
```

Registrar una salida desde `ms-ventas`:

```http
POST /api/inventario/stock/salida
Authorization: Bearer <jwt-cliente-o-token-interno>
Content-Type: application/json

{
  "productoId": 1,
  "cantidad": 4,
  "referencia": "VENTA-928"
}
```

Respuesta de una operacion aplicada:

```json
{
  "movimientoId": 18,
  "productoId": 1,
  "sku": "SEM-001",
  "tipoMovimiento": "SALIDA",
  "cantidad": 4,
  "stockAnterior": 120,
  "stockPosterior": 116,
  "referencia": "VENTA-928",
  "duplicada": false,
  "fecha": "2026-08-24T20:00:00Z"
}
```

Un reintento de la misma operacion responde `200 OK`, conserva los valores del
movimiento original y cambia `duplicada` a `true`.

Ejemplo de stock insuficiente:

```http
HTTP/1.1 409 Conflict
Content-Type: application/json
```

```json
{
  "timestamp": "2026-08-24T20:00:00Z",
  "status": 409,
  "error": "CONFLICT",
  "message": "Stock insuficiente para el producto SEM-001",
  "path": "/api/inventario/stock/salida",
  "validationErrors": {}
}
```

Las respuestas `400`, `401`, `403`, `404`, `409` y `500` utilizan la misma
estructura y nunca incluyen stack traces.

## Flujo de seguridad

```text
React
-> Cognito (Authorization Code con PKCE)
-> JWT
-> AWS API Gateway
-> bff-web
-> ms-inventario
```

API Gateway valida perimetralmente firma, JWKS, issuer, audience y vigencia. El
BFF valida los permisos de la operacion. Como defensa en profundidad,
`ms-inventario` vuelve a validar el JWT como OAuth2 Resource Server:

- firma criptografica con las claves JWKS de Cognito;
- `iss`, `aud`, `exp` y `nbf`;
- grupos `cognito:groups` y `custom:role` como `ROLE_CLIENTE`/`ROLE_ADMIN`;
- scopes `inventario.stock.read` y `inventario.stock.write` para integraciones.

El contenedor debe desplegarse en la subred privada de computo, sin IP publica.
El frontend no debe conocer ni consumir directamente su URL.

## Roles

`CLIENTE` puede consultar productos, stock, disponibilidad y participar en el
flujo de salida orquestado por `ms-ventas`. No puede crear/editar productos,
registrar entradas ni consultar la auditoria administrativa.

`ADMIN` puede administrar productos, registrar entradas, consultar todo el
inventario y revisar movimientos. Los scopes internos permiten preparar tokens
de servicio con minimo privilegio cuando se habilite autenticacion maquina a
maquina.

## Comunicacion con BFF

El contrato se consume exclusivamente mediante API REST:

```text
React -> API Gateway -> bff-web -> ms-inventario
```

El BFF propaga el Bearer token y expone al frontend solamente los endpoints que
corresponden a su perfil. `ms-inventario` no configura CORS para navegadores
porque no es un destino publico.

## Comunicacion con ms-compras

```text
ms-compras
-> POST /api/inventario/stock/entrada
-> referencia COMPRA-<id>
-> ms-inventario aumenta stock y registra ENTRADA
```

La referencia permite reintentos seguros. `ms-compras` no accede a las tablas de
inventario.

## Comunicacion con ms-ventas

```text
ms-ventas
-> POST /api/inventario/stock/validar
-> POST /api/inventario/stock/salida
-> referencia VENTA-<id>
-> ms-inventario descuenta stock y registra SALIDA
```

`ms-ventas` no accede a `db_inventario`. Si el stock ya no es suficiente, la
salida responde `409 Conflict` y la transaccion comercial debe manejar el fallo.

## Flujo de inventario

```text
ms-compras -> ENTRADA -> ms-inventario -> aumenta stock
```

```text
ms-ventas -> valida disponibilidad -> ms-inventario
ms-ventas -> SALIDA -> ms-inventario -> disminuye stock
```

Cada cambio genera un registro inmutable de `MovimientoInventario` con cantidad,
stock anterior, stock posterior, referencia, origen y fecha. Los valores
`AJUSTE` y `RESERVA` existen en el dominio para una siguiente etapa, pero el MVP
solo expone operaciones completas de `ENTRADA` y `SALIDA`.

## Tests

La suite utiliza H2 exclusivamente bajo el perfil `test`; no necesita una base
PostgreSQL local. Cubre:

- creacion y consulta de productos;
- validaciones de DTO;
- entradas, salidas y stock insuficiente;
- imposibilidad de stock negativo;
- generacion de movimientos;
- idempotencia;
- dos ventas concurrentes sobre el mismo producto;
- autorizacion CLIENTE y ADMIN;
- emision, validacion y permisos de JWT locales bajo el perfil `dev`;
- ausencia del emisor local fuera del perfil `dev`;
- rechazo sin JWT;
- health check sin autenticacion.

Ejecutar pruebas:

```powershell
.\mvnw.cmd test
```

Compilar y empaquetar:

```powershell
.\mvnw.cmd clean package
```

El artefacto queda en `target/ms-inventario-0.0.1-SNAPSHOT.jar`.
