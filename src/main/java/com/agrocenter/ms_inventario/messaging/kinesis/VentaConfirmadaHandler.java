package com.agrocenter.ms_inventario.messaging.kinesis;

import com.agrocenter.ms_inventario.dto.OperacionStockRequest;
import com.agrocenter.ms_inventario.messaging.event.OrdenCompraEvent;
import com.agrocenter.ms_inventario.service.InventarioService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class VentaConfirmadaHandler {
    private final EventoProcesadoRepository procesados;
    private final InventarioService inventario;

    /** La marca y todos los movimientos participan en la misma transacción. */
    @Transactional
    public void procesar(String eventId, OrdenCompraEvent event) {
        if (eventId == null || eventId.isBlank() || event == null || event.ventaId() == null
                || event.items() == null || event.items().isEmpty()) {
            throw new EventoInvalidoException("Evento de venta invalido");
        }
        if (procesados.existsById(eventId)) return;
        procesados.saveAndFlush(new EventoProcesado(eventId));
        for (var item : event.items()) {
            if (item == null || item.productoId() == null || item.cantidad() == null || item.cantidad() <= 0) {
                throw new EventoInvalidoException("Item de venta invalido");
            }
            inventario.registrarSalida(new OperacionStockRequest(item.productoId(), item.cantidad(), "VENTA-" + event.ventaId()));
        }
    }
}
