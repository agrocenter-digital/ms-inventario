package com.agrocenter.ms_inventario.messaging;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.assertThat;
import com.agrocenter.ms_inventario.messaging.audit.AuditoriaEventos;
import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoDetectado;
import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoEvent;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

class InventarioCriticoNotifierSwitchTest {
    @Test void banderaDesactivadaNoEnviaYPorDefectoConservaKafka() {
        @SuppressWarnings("unchecked") KafkaTemplate<String, InventarioCriticoEvent> kafka = mock(KafkaTemplate.class);
        var notifier = new InventarioCriticoNotifier(kafka, mock(AuditoriaEventos.class));
        assertThat(ReflectionTestUtils.getField(notifier, "kafkaEnabled")).isEqualTo(true);
        var evento = new InventarioCriticoDetectado(1L, "SKU", "Producto", 1, 3);
        ReflectionTestUtils.setField(notifier, "kafkaEnabled", false);
        notifier.publicarInventarioCritico(evento);
        verifyNoInteractions(kafka);
        ReflectionTestUtils.setField(notifier, "kafkaEnabled", true);
        when(kafka.send(anyString(), anyString(), any(InventarioCriticoEvent.class))).thenReturn(new CompletableFuture<>());
        notifier.publicarInventarioCritico(evento);
        verify(kafka).send(eq(KafkaTopics.INVENTARIO_CRITICO), eq("1"), any(InventarioCriticoEvent.class));
    }
}
