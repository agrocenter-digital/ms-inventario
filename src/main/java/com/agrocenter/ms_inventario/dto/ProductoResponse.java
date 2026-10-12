package com.agrocenter.ms_inventario.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record ProductoResponse(
        Long id,
        String sku,
        String nombre,
        String descripcion,
        String categoria,
        BigDecimal precioVenta,
        Integer stockActual,
        Integer stockMinimo,
        boolean stockBajo,
        boolean activo,
        Instant createdAt,
        Instant updatedAt,
        boolean soloCotizacion
) {
    // Constructor compatible para consumidores Java existentes.
    public ProductoResponse(Long id, String sku, String nombre, String descripcion, String categoria,
            BigDecimal precioVenta, Integer stockActual, Integer stockMinimo,
            boolean stockBajo, boolean activo, Instant createdAt, Instant updatedAt) {
        this(id, sku, nombre, descripcion, categoria, precioVenta, stockActual, stockMinimo,
                stockBajo, activo, createdAt, updatedAt, false);
    }

}
