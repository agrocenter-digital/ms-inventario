# Modalidad comercial sin migración

`CATALOG_QUOTE_ONLY_SKUS` configura una lista de SKU aprobados separados por coma.
Todos los productos cuya SKU coincida exactamente (ignorando mayúsculas/espacios
alrededor de cada entrada) exponen `soloCotizacion: true` en `ProductoResponse`.
El valor vacío conserva el catálogo minorista previo. No hay clasificación por
categoría, nombre ni monto, y no se modifica la entidad ni el esquema de datos.

Configurar la misma lista en todas las réplicas y reiniciar/re desplegar tras
cambios. `compose.yaml` transmite la variable del `.env`. En ECS u otro despliegue,
agregarla al entorno del contenedor. El administrador comercial debe aprobar los
SKU antes de ofrecer maquinaria/mayoristas, pues no se dispone de esa información
en los datos actuales. No hay SKU cotizables ficticios por defecto.

Requiere las versiones de BFF que propagan el campo y de ms-ventas que rechazan
`soloCotizacion=true` antes de crear una venta. Desplegar esos consumidores primero.
Consultar el README del frontend para el flujo de contacto y configuración pública.
