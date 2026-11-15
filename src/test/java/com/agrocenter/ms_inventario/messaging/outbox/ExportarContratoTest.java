package com.agrocenter.ms_inventario.messaging.outbox;

import com.agrocenter.ms_inventario.messaging.event.InventarioCriticoDetectado;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "CONTRATOS_DIR", matches = ".+")
class ExportarContratoTest {
    @Autowired StockCriticoOutboxWriter writer;
    @Autowired OutboxRepository repository;

    @Test @Transactional void exportaSobreRealDeStockCritico() throws Exception {
        writer.registrar(new InventarioCriticoDetectado(10L, "SKU-TEST", "Producto", 1, 3));
        var evento = repository.findAll().stream().filter(e -> "InventarioCritico".equals(e.getEventType())).findFirst().orElseThrow();
        Files.writeString(Path.of(System.getenv("CONTRATOS_DIR"), "stock.json"), evento.getPayload());
    }
}
