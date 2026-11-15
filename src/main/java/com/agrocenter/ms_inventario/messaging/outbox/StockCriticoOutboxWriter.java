package com.agrocenter.ms_inventario.messaging.outbox;

import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoDetectado;
import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoEvent;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class StockCriticoOutboxWriter {
    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void registrar(InventarioCriticoDetectado detectado) {
        Instant now = Instant.now();
        int actual = detectado.stockActual() == null ? 0 : detectado.stockActual();
        int minimo = detectado.stockMinimo() == null ? 0 : detectado.stockMinimo();
        var payload = new InventarioCriticoEvent(detectado.productoId(), detectado.sku(), detectado.nombre(),
                detectado.stockActual(), detectado.stockMinimo(), Math.max(minimo * 2 - actual, 1), now);
        String id = UUID.randomUUID().toString();
        var event = new EventEnvelope(id, "InventarioCritico", 1, now, "ms-inventario", "Producto",
                String.valueOf(detectado.productoId()), MDC.get("correlationId"), payload);
        repository.save(new OutboxEvent(id, event.eventType(), event.aggregateId(),
                "producto:" + detectado.productoId(), mapper.writeValueAsString(event), now));
    }

    public record EventEnvelope(String eventId, String eventType, int schemaVersion, Instant occurredAt,
                                String producer, String aggregateType, String aggregateId, String correlationId,
                                InventarioCriticoEvent payload) { }
}
