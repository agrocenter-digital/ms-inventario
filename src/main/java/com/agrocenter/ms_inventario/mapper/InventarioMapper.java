package com.agrocenter.ms_inventario.mapper;

import com.agrocenter.ms_inventario.dto.MovimientoResponse;
import com.agrocenter.ms_inventario.dto.OperacionStockResponse;
import com.agrocenter.ms_inventario.dto.ProductoResponse;
import com.agrocenter.ms_inventario.dto.ProductoStockResponse;
import com.agrocenter.ms_inventario.entity.MovimientoInventario;
import com.agrocenter.ms_inventario.entity.Producto;
import org.springframework.stereotype.Component;

@Component
public class InventarioMapper {
    @org.springframework.beans.factory.annotation.Value("${agrocenter.catalogo.skus-solo-cotizacion:}")
    private String skusSoloCotizacion = "";

    private boolean requiereCotizacion(String sku) {
        return java.util.Arrays.stream(skusSoloCotizacion.split(","))
                .map(String::trim).filter(value -> !value.isEmpty())
                .anyMatch(value -> value.equalsIgnoreCase(sku));
    }


    public ProductoResponse toProductoResponse(Producto producto) {
        return new ProductoResponse(
                producto.getId(),
                producto.getSku(),
                producto.getNombre(),
                producto.getDescripcion(),
                producto.getCategoria(),
                producto.getPrecioVenta(),
                producto.getStockActual(),
                producto.getStockMinimo(),
                producto.tieneStockBajo(),
                producto.isActivo(),
                producto.getCreatedAt(),
                producto.getUpdatedAt(),
                requiereCotizacion(producto.getSku())
        );
    }

    public ProductoStockResponse toStockResponse(Producto producto) {
        return new ProductoStockResponse(
                producto.getId(),
                producto.getSku(),
                producto.getNombre(),
                producto.getStockActual(),
                producto.getStockMinimo(),
                producto.tieneStockBajo()
        );
    }

    public MovimientoResponse toMovimientoResponse(MovimientoInventario movimiento) {
        return new MovimientoResponse(
                movimiento.getId(),
                movimiento.getProducto().getId(),
                movimiento.getProducto().getSku(),
                movimiento.getTipoMovimiento(),
                movimiento.getCantidad(),
                movimiento.getStockAnterior(),
                movimiento.getStockPosterior(),
                movimiento.getReferencia(),
                movimiento.getOrigen(),
                movimiento.getFecha()
        );
    }

    public OperacionStockResponse toOperacionStockResponse(
            MovimientoInventario movimiento,
            boolean duplicada
    ) {
        return new OperacionStockResponse(
                movimiento.getId(),
                movimiento.getProducto().getId(),
                movimiento.getProducto().getSku(),
                movimiento.getTipoMovimiento(),
                movimiento.getCantidad(),
                movimiento.getStockAnterior(),
                movimiento.getStockPosterior(),
                movimiento.getReferencia(),
                duplicada,
                movimiento.getFecha()
        );
    }
}
