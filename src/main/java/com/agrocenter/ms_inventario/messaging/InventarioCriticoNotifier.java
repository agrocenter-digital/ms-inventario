package com.agrocenter.ms_inventario.messaging;

import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoDetectado;
import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventarioCriticoNotifier {

    private final KafkaTemplate<String, InventarioCriticoEvent> kafkaTemplate;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publicarInventarioCritico(InventarioCriticoDetectado detectado) {
        int cantidadSugerida = calcularCantidadSugerida(
                detectado.stockActual(),
                detectado.stockMinimo()
        );
        InventarioCriticoEvent event = new InventarioCriticoEvent(
                detectado.productoId(),
                detectado.sku(),
                detectado.nombre(),
                detectado.stockActual(),
                detectado.stockMinimo(),
                cantidadSugerida,
                Instant.now()
        );

        kafkaTemplate.send(KafkaTopics.INVENTARIO_CRITICO, String.valueOf(detectado.productoId()), event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error(
                                "No fue posible publicar alerta de inventario critico productoId={}",
                                detectado.productoId(),
                                exception
                        );
                        return;
                    }
                    log.info(
                            "Alerta de inventario critico publicada productoId={} stockActual={} stockMinimo={} cantidadSugerida={}",
                            detectado.productoId(),
                            detectado.stockActual(),
                            detectado.stockMinimo(),
                            cantidadSugerida
                    );
                });
    }

    private int calcularCantidadSugerida(Integer stockActual, Integer stockMinimo) {
        int actual = stockActual == null ? 0 : stockActual;
        int minimo = stockMinimo == null ? 0 : stockMinimo;
        return Math.max(minimo * 2 - actual, 1);
    }
}
