package com.agrocenter.ms_inventario.messaging.outbox;

import com.agrocenter.ms_inventario.entity.MovimientoInventario;
import com.agrocenter.ms_inventario.entity.Producto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class InventarioOutboxWriter {
    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    @Transactional(propagation = Propagation.MANDATORY)
    public void registrarMovimiento(MovimientoInventario movimiento) {
        Producto producto = movimiento.getProducto();
        var payload = new MovimientoPayload(movimiento.getId(), producto.getId(), producto.getSku(),
                movimiento.getTipoMovimiento().name(), movimiento.getCantidad(), movimiento.getStockAnterior(),
                movimiento.getStockPosterior(), movimiento.getOrigen().name());
        registrar("MovimientoInventarioRegistrado", producto.getId(), payload);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void registrarProducto(String tipo, Producto producto, Integer stockAnterior) {
        var payload = new ProductoPayload(producto.getId(), producto.getSku(), producto.getNombre(),
                producto.getCategoria(), producto.getPrecioVenta(), stockAnterior, producto.getStockActual(),
                producto.getStockMinimo(), producto.isActivo());
        registrar(tipo, producto.getId(), payload);
    }

    private void registrar(String tipo, Long productoId, Object payload) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        var event = new EventEnvelope(id, tipo, 1, now, "ms-inventario", "Producto",
                String.valueOf(productoId), MDC.get("correlationId"), payload);
        repository.save(new OutboxEvent(id, tipo, String.valueOf(productoId), "producto:" + productoId,
                mapper.writeValueAsString(event), now));
    }

    public record EventEnvelope(String eventId, String eventType, int schemaVersion, Instant occurredAt,
                                String producer, String aggregateType, String aggregateId, String correlationId,
                                Object payload) { }
    public record MovimientoPayload(Long movimientoId, Long productoId, String sku, String tipoMovimiento,
                                    Integer cantidad, Integer stockAnterior, Integer stockPosterior, String origen) { }
    public record ProductoPayload(Long productoId, String sku, String nombre, String categoria, BigDecimal precioVenta,
                                  Integer stockAnterior, Integer stockActual, Integer stockMinimo, boolean activo) { }
}
