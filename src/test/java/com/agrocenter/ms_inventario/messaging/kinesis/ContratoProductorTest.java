package com.agrocenter.ms_inventario.messaging.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tools.jackson.databind.json.JsonMapper;

@EnabledIfEnvironmentVariable(named = "CONTRATOS_DIR", matches = ".+")
class ContratoProductorTest {
    @Test void consumidorAceptaVentaGeneradaPorVentas() throws Exception {
        var handler = mock(VentaConfirmadaHandler.class);
        var processor = new EnvelopeProcessor(new JsonMapper(), handler);
        assertThat(processor.procesar(Files.readAllBytes(Path.of(System.getenv("CONTRATOS_DIR"), "venta.json")))).isTrue();
        verify(handler).procesar(anyString(), argThat(venta -> venta.ventaId() == 42L && venta.items().getFirst().cantidad() == 2));
        assertThat(processor.procesar(Files.readAllBytes(Path.of(System.getenv("CONTRATOS_DIR"), "b2b-creada.json")))).isFalse();
        assertThat(processor.procesar(Files.readAllBytes(Path.of(System.getenv("CONTRATOS_DIR"), "CompraCreada.json")))).isFalse();
        verifyNoMoreInteractions(handler);
    }
}
