package com.agrocenter.ms_inventario.messaging;

import com.agrocenter.ms_inventario.messaging.audit.AuditoriaEventos;
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
    private final AuditoriaEventos auditoriaEventos;

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

        String clave = String.valueOf(detectado.productoId());
        try {
            kafkaTemplate.send(KafkaTopics.INVENTARIO_CRITICO, clave, event)
                    .whenComplete((result, exception) -> {
                        if (exception != null) {
                            auditoriaEventos.registrar(
                                    KafkaTopics.INVENTARIO_CRITICO,
                                    AuditoriaEventos.EMITIDO,
                                    clave,
                                    AuditoriaEventos.ERROR,
                                    "productoId=" + detectado.productoId()
                                            + " error=" + exception.getMessage()
                            );
                            log.error(
                                    "No fue posible publicar alerta de inventario critico productoId={}",
                                    detectado.productoId(),
                                    exception
                            );
                            return;
                        }
                        auditoriaEventos.registrar(
                                KafkaTopics.INVENTARIO_CRITICO,
                                AuditoriaEventos.EMITIDO,
                                clave,
                                AuditoriaEventos.EXITO,
                                "productoId=" + detectado.productoId()
                                        + " stockActual=" + detectado.stockActual()
                                        + " cantidadSugerida=" + cantidadSugerida
                        );
                        log.info(
                                "Alerta de inventario critico publicada productoId={} stockActual={} stockMinimo={} cantidadSugerida={}",
                                detectado.productoId(),
                                detectado.stockActual(),
                                detectado.stockMinimo(),
                                cantidadSugerida
                        );
                    });
        } catch (RuntimeException exception) {
            auditoriaEventos.registrar(
                    KafkaTopics.INVENTARIO_CRITICO,
                    AuditoriaEventos.EMITIDO,
                    clave,
                    AuditoriaEventos.ERROR,
                    "productoId=" + detectado.productoId() + " error=" + exception.getMessage()
            );
            throw exception;
        }
    }

    private int calcularCantidadSugerida(Integer stockActual, Integer stockMinimo) {
        int actual = stockActual == null ? 0 : stockActual;
        int minimo = stockMinimo == null ? 0 : stockMinimo;
        return Math.max(minimo * 2 - actual, 1);
    }
}
