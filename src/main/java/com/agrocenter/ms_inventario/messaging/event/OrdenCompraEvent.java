package com.agrocenter.ms_inventario.messaging.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrdenCompraEvent(
        Long ventaId,
        String clienteId,
        Instant fechaCreacion,
        BigDecimal total,
        List<ItemOrdenCompraEvent> items
) {

    public record ItemOrdenCompraEvent(
            Long productoId,
            String sku,
            Integer cantidad,
            BigDecimal precioUnitario,
            BigDecimal subtotal
    ) {
    }
}
