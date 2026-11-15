package com.agrocenter.ms_inventario.messaging.kinesis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import tools.jackson.databind.json.JsonMapper;

class EnvelopeProcessorTest {
    @Test void propagaCorrelacionSeguraYRestauraContextoInclusoAnteFallo() {
        var handler = mock(VentaConfirmadaHandler.class);
        var processor = new EnvelopeProcessor(new JsonMapper(), handler);
        String plantilla = """
                {"eventId":"evt-1","eventType":"VentaConfirmada","schemaVersion":1,"correlationId":"%s",
                 "payload":{"ventaId":12,"clienteId":"c","fechaCreacion":"2026-11-12T22:00:00Z",
                 "total":100,"items":[{"productoId":5,"sku":"S","cantidad":1,"precioUnitario":100,"subtotal":100}]}}
                """;
        MDC.put("correlationId", "contexto-previo");
        try {
            doAnswer(invocation -> { assertThat(MDC.get("correlationId")).isEqualTo("traza-123"); return null; })
                    .doAnswer(invocation -> { assertThat(MDC.get("correlationId")).isEqualTo("evt-1"); throw new IllegalStateException("fallo"); })
                    .when(handler).procesar(any(), any());
            processor.procesar(json(plantilla.formatted("traza-123")));
            assertThat(MDC.get("correlationId")).isEqualTo("contexto-previo");
            assertThatThrownBy(() -> processor.procesar(json(plantilla.formatted("valor inseguro")))).isInstanceOf(IllegalStateException.class);
            assertThat(MDC.get("correlationId")).isEqualTo("contexto-previo");
        } finally { MDC.remove("correlationId"); }
    }
    @Test void procesaContratoVersionadoYOtrosTiposSeOmiten() {
        var handler = mock(VentaConfirmadaHandler.class);
        var processor = new EnvelopeProcessor(new JsonMapper(), handler);
        processor.procesar(json("""
                {"eventId":"evt-1","eventType":"VentaConfirmada","schemaVersion":1,
                 "payload":{"ventaId":12,"clienteId":"c","fechaCreacion":"2026-11-12T22:00:00Z",
                 "total":100,"items":[{"productoId":5,"sku":"S","cantidad":1,"precioUnitario":100,"subtotal":100}]}}
                """));
        verify(handler).procesar(eq("evt-1"), any());
        processor.procesar(json("{\"eventType\":\"ConsultaB2BCreada\"}"));
        verifyNoMoreInteractions(handler);
    }

    @Test void contratoInvalidoNoSeOmite() {
        var processor = new EnvelopeProcessor(new JsonMapper(), mock(VentaConfirmadaHandler.class));
        assertThatThrownBy(() -> processor.procesar(json("{\"eventType\":\"VentaConfirmada\",\"schemaVersion\":2}")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private byte[] json(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
