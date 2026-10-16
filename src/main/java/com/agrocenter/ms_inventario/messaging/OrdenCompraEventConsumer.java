package com.agrocenter.ms_inventario.messaging;

import com.agrocenter.ms_inventario.dto.OperacionStockRequest;
import com.agrocenter.ms_inventario.dto.OperacionStockResponse;
import com.agrocenter.ms_inventario.messaging.audit.AuditoriaEventos;
import com.agrocenter.ms_inventario.messaging.event.OrdenCompraEvent;
import com.agrocenter.ms_inventario.service.InventarioService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrdenCompraEventConsumer {

    private static final String REFERENCIA_VENTA_PREFIX = "VENTA-";

    private final InventarioService inventarioService;
    private final AuditoriaEventos auditoriaEventos;

    @KafkaListener(topics = KafkaTopics.ORDENES_COMPRAS)
    public void onOrdenCompra(OrdenCompraEvent event) {
        if (event == null || event.ventaId() == null || event.items() == null || event.items().isEmpty()) {
            log.warn("Evento de orden de compra invalido ignorado: {}", event);
            auditoriaEventos.registrar(
                    KafkaTopics.ORDENES_COMPRAS,
                    AuditoriaEventos.CONSUMIDO,
                    "desconocido",
                    AuditoriaEventos.ERROR,
                    "Evento de orden de compra invalido o sin items"
            );
            return;
        }

        String referencia = REFERENCIA_VENTA_PREFIX + event.ventaId();
        log.info(
                "Procesando orden de compra ventaId={} items={} referencia={}",
                event.ventaId(),
                event.items().size(),
                referencia
        );

        int duplicados = 0;
        int errores = 0;
        for (OrdenCompraEvent.ItemOrdenCompraEvent item : event.items()) {
            try {
                OperacionStockResponse respuesta = inventarioService.registrarSalida(
                        new OperacionStockRequest(item.productoId(), item.cantidad(), referencia)
                );
                if (respuesta.duplicada()) {
                    duplicados++;
                    log.info(
                            "Descuento ya aplicado previamente productoId={} referencia={}",
                            item.productoId(),
                            referencia
                    );
                }
            } catch (RuntimeException exception) {
                errores++;
                log.error(
                        "No fue posible descontar stock por evento productoId={} referencia={}",
                        item.productoId(),
                        referencia,
                        exception
                );
            }
        }

        auditoriaEventos.registrar(
                KafkaTopics.ORDENES_COMPRAS,
                AuditoriaEventos.CONSUMIDO,
                String.valueOf(event.ventaId()),
                errores == 0 ? AuditoriaEventos.EXITO : AuditoriaEventos.ERROR,
                "referencia=" + referencia
                        + " items=" + event.items().size()
                        + " duplicados=" + duplicados
                        + " errores=" + errores
        );
    }
}
