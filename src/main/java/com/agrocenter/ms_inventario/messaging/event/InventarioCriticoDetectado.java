package com.agrocenter.ms_inventario.messaging.event;

public record InventarioCriticoDetectado(
        Long productoId,
        String sku,
        String nombre,
        Integer stockActual,
        Integer stockMinimo
) {
}
