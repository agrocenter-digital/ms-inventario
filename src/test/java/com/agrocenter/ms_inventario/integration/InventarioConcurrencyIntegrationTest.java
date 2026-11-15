package com.agrocenter.ms_inventario.integration;

import com.agrocenter.ms_inventario.dto.OperacionStockRequest;
import com.agrocenter.ms_inventario.dto.ProductoCreateRequest;
import com.agrocenter.ms_inventario.dto.ProductoResponse;
import com.agrocenter.ms_inventario.dto.ProductoUpdateRequest;
import com.agrocenter.ms_inventario.exception.ConflictoNegocioException;
import com.agrocenter.ms_inventario.repository.MovimientoInventarioRepository;
import com.agrocenter.ms_inventario.repository.ProductoRepository;
import com.agrocenter.ms_inventario.service.InventarioService;
import com.agrocenter.ms_inventario.service.ProductoService;
import com.agrocenter.ms_inventario.messaging.outbox.OutboxRepository;
import com.agrocenter.ms_inventario.messaging.kinesis.EventoProcesadoRepository;
import com.agrocenter.ms_inventario.messaging.kinesis.VentaConfirmadaHandler;
import com.agrocenter.ms_inventario.messaging.kinesis.CuarentenaRepository;
import com.agrocenter.ms_inventario.messaging.kinesis.CuarentenaService;
import com.agrocenter.ms_inventario.messaging.kinesis.EventoInvalidoException;
import com.agrocenter.ms_inventario.messaging.kinesis.EnvelopeProcessor;
import com.agrocenter.ms_inventario.messaging.kinesis.ReprocesamientoCuarentena;
import com.agrocenter.ms_inventario.messaging.event.OrdenCompraEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class InventarioConcurrencyIntegrationTest {

    @Autowired
    private ProductoService productoService;

    @Autowired
    private InventarioService inventarioService;

    @Autowired
    private MovimientoInventarioRepository movimientoRepository;

    @Autowired
    private ProductoRepository productoRepository;

    @Autowired private OutboxRepository outboxRepository;
    @Autowired private TransactionTemplate transactionTemplate;
    @Autowired private EventoProcesadoRepository eventosProcesados;
    @Autowired private VentaConfirmadaHandler ventaHandler;
    @Autowired private CuarentenaService cuarentena;
    @Autowired private CuarentenaRepository cuarentenaRepository;
    @Autowired private ReprocesamientoCuarentena reprocesamiento;
    @Autowired private EnvelopeProcessor envelopeProcessor;

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        outboxRepository.deleteAll();
        eventosProcesados.deleteAll();
        cuarentenaRepository.deleteAll();
        movimientoRepository.deleteAll();
        productoRepository.deleteAll();
        executor = Executors.newFixedThreadPool(2);
    }

    @Test void cuarentenaEsDurableYNoDuplicaMismoRegistro() {
        cuarentena.guardar("stream", "shard", "123", 0L, "invalido".getBytes(), new EventoInvalidoException("error"), 1);
        cuarentena.guardar("stream", "shard", "123", 0L, "invalido".getBytes(), new EventoInvalidoException("error"), 1);
        assertThat(cuarentenaRepository.count()).isEqualTo(1);
        cuarentena.guardar("stream", "shard", "123", 1L, "otro".getBytes(), new EventoInvalidoException("error"), 1);
        cuarentena.guardar("stream", "shard", "123", 1L, "otro".getBytes(), new EventoInvalidoException("error"), 1);
        assertThat(cuarentenaRepository.count()).isEqualTo(2);
        assertThat(cuarentenaRepository.findAll()).extracting(e -> e.getSubSequenceNumber())
                .containsExactlyInAnyOrder(0L, 1L);
    }

    @Test void reprocesamientoCorrigeEventoSinDuplicarDescuento() {
        ProductoResponse producto = crearProducto("REPLAY-001");
        inventarioService.registrarEntrada(new OperacionStockRequest(producto.id(), 10, "COMPRA-REPLAY"));
        cuarentena.guardar("stream", "shard", "124", 0L, "malformado".getBytes(), new EventoInvalidoException("error"), 1);
        String id = cuarentenaRepository.findAll().getFirst().getId();
        byte[] payload = ("""
                {"eventId":"replay-1","eventType":"VentaConfirmada","schemaVersion":1,
                 "payload":{"ventaId":104,"clienteId":"c","fechaCreacion":"2026-11-12T22:00:00Z",
                 "total":2,"items":[{"productoId":%d,"sku":"REPLAY-001","cantidad":2,"precioUnitario":1,"subtotal":2}]}}
                """).formatted(producto.id()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> reprocesamiento.reprocesar(id, "{}".getBytes()))
                .isInstanceOf(EventoInvalidoException.class);
        assertThat(cuarentenaRepository.findById(id).orElseThrow().getResueltoEn()).isNull();
        assertThat(reprocesamiento.reprocesar(id, payload)).isEqualTo(ReprocesamientoCuarentena.Resultado.APLICADO);
        assertThat(reprocesamiento.reprocesar(id, "otro".getBytes())).isEqualTo(ReprocesamientoCuarentena.Resultado.YA_RESUELTO);
        assertThat(cuarentenaRepository.findById(id).orElseThrow().getResueltoEn()).isNotNull();
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(8);
        assertThat(eventosProcesados.count()).isEqualTo(1);
    }

    @Test void correccionesConcurrentesDelMismoIncidenteProducenUnSoloEfecto() throws Exception {
        ProductoResponse producto = crearProducto("REPLAY-RACE");
        inventarioService.registrarEntrada(new OperacionStockRequest(producto.id(), 10, "COMPRA-RACE"));
        cuarentena.guardar("stream", "shard", "125", 0L, "malformado".getBytes(), new EventoInvalidoException("error"), 1);
        String id = cuarentenaRepository.findAll().getFirst().getId();
        String plantilla = """
                {"eventId":"replay-race-%d","eventType":"VentaConfirmada","schemaVersion":1,
                 "payload":{"ventaId":%d,"clienteId":"c","fechaCreacion":"2026-11-12T22:00:00Z",
                 "total":2,"items":[{"productoId":%d,"sku":"REPLAY-RACE","cantidad":2,"precioUnitario":1,"subtotal":2}]}}
                """;
        CountDownLatch listo = new CountDownLatch(2);
        CountDownLatch iniciar = new CountDownLatch(1);
        Callable<ReprocesamientoCuarentena.Resultado> primero = () -> { listo.countDown(); iniciar.await(); return reprocesamiento.reprocesar(id, plantilla.formatted(1, 201, producto.id()).getBytes(java.nio.charset.StandardCharsets.UTF_8)); };
        Callable<ReprocesamientoCuarentena.Resultado> segundo = () -> { listo.countDown(); iniciar.await(); return reprocesamiento.reprocesar(id, plantilla.formatted(2, 202, producto.id()).getBytes(java.nio.charset.StandardCharsets.UTF_8)); };
        Future<ReprocesamientoCuarentena.Resultado> a = executor.submit(primero);
        Future<ReprocesamientoCuarentena.Resultado> b = executor.submit(segundo);
        assertThat(listo.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        iniciar.countDown();
        assertThat(List.of(a.get(20, java.util.concurrent.TimeUnit.SECONDS), b.get(20, java.util.concurrent.TimeUnit.SECONDS)))
                .containsExactlyInAnyOrder(ReprocesamientoCuarentena.Resultado.APLICADO, ReprocesamientoCuarentena.Resultado.YA_RESUELTO);
        assertThat(eventosProcesados.count()).isEqualTo(1);
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(8);
        assertThat(cuarentenaRepository.findById(id).orElseThrow().getResueltoEn()).isNotNull();
    }

    @Test
    void consumidorNoDuplicaStockYRevierteAnteItemInvalido() {
        ProductoResponse producto = crearProducto("KCL-001");
        inventarioService.registrarEntrada(new OperacionStockRequest(producto.id(), 10, "COMPRA-KCL"));
        var item = new OrdenCompraEvent.ItemOrdenCompraEvent(producto.id(), "KCL-001", 2, BigDecimal.ONE, BigDecimal.valueOf(2));
        var event = new OrdenCompraEvent(101L, "cliente", java.time.Instant.now(), BigDecimal.valueOf(2), List.of(item));
        ventaHandler.procesar("event-101", event);
        ventaHandler.procesar("event-101", event);
        assertThat(eventosProcesados.count()).isEqualTo(1);
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(8);

        var invalido = new OrdenCompraEvent.ItemOrdenCompraEvent(producto.id(), "KCL-001", 0, BigDecimal.ONE, BigDecimal.ZERO);
        var fallido = new OrdenCompraEvent(102L, "cliente", java.time.Instant.now(), BigDecimal.valueOf(2), List.of(item, invalido));
        assertThatThrownBy(() -> ventaHandler.procesar("event-102", fallido)).isInstanceOf(IllegalArgumentException.class);
        assertThat(eventosProcesados.existsById("event-102")).isFalse();
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(8);
    }

    @Test
    void guardaAlertaCriticaUnaVezYRevierteEnRollback() {
        ProductoResponse producto = crearProducto("OUT-001");
        inventarioService.registrarEntrada(new OperacionStockRequest(producto.id(), 5, "COMPRA-OUT"));
        outboxRepository.deleteAll();
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            inventarioService.registrarSalida(new OperacionStockRequest(producto.id(), 4, "VENTA-ROLLBACK"));
            throw new IllegalStateException("rollback simulado");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(outboxRepository.count()).isZero();
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(5);

        var request = new OperacionStockRequest(producto.id(), 4, "VENTA-OUT");
        inventarioService.registrarSalida(request);
        inventarioService.registrarSalida(request);
        assertThat(outboxRepository.findAll()).extracting(event -> event.getEventType())
                .containsExactlyInAnyOrder("MovimientoInventarioRegistrado", "InventarioCritico");
        assertThat(outboxRepository.findAll()).allSatisfy(event -> assertThat(event.getPayload()).contains("OUT-001"));
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(1);
    }

    @Test void eventosAdministrativosYMovimientosSonTransaccionalesYNoDuplican() {
        ProductoResponse producto = crearProducto("EVENT-001");
        var entrada = new OperacionStockRequest(producto.id(), 5, "COMPRA-EVENTO");
        inventarioService.registrarEntrada(entrada);
        inventarioService.registrarEntrada(entrada);
        productoService.actualizar(producto.id(), new ProductoUpdateRequest("EVENT-001", "Actualizado", "privado",
                "Categoria", BigDecimal.TEN, 2, 3));
        productoService.cambiarEstado(producto.id(), false);
        productoService.cambiarEstado(producto.id(), false);
        var eventos = outboxRepository.findAll();
        assertThat(eventos).extracting(e -> e.getEventType()).containsExactlyInAnyOrder(
                "ProductoCreado", "MovimientoInventarioRegistrado", "ProductoActualizado", "ProductoEstadoActualizado");
        assertThat(eventos.stream().sorted(java.util.Comparator.comparing(e -> e.getOrden())).map(e -> e.getEventType()).toList())
                .containsExactly("ProductoCreado", "MovimientoInventarioRegistrado", "ProductoActualizado", "ProductoEstadoActualizado");
        assertThat(eventos).allSatisfy(e -> {
            assertThat(e.getPartitionKey()).isEqualTo("producto:" + producto.id());
            assertThat(e.getPayload()).doesNotContain("privado", "COMPRA-EVENTO");
        });
        assertThat(eventos.stream().filter(e -> e.getEventType().equals("ProductoActualizado"))
                .findFirst().orElseThrow().getPayload()).contains("\"stockAnterior\":5", "\"stockActual\":3");
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            productoService.cambiarEstado(producto.id(), true);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(productoService.obtenerPorId(producto.id(), true).activo()).isFalse();
        assertThat(outboxRepository.count()).isEqualTo(4);
    }

    @Test void crearProductoYEventoSeReviertenJuntos() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            crearProducto("ROLLBACK-NEW");
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(productoRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test void ventaCorrelacionaMovimientoYAlertaSinDuplicar() {
        ProductoResponse producto = crearProducto("TRACE-001");
        inventarioService.registrarEntrada(new OperacionStockRequest(producto.id(), 5, "COMPRA-TRACE"));
        outboxRepository.deleteAll();
        byte[] evento = ("""
                {"eventId":"trace-event-1","eventType":"VentaConfirmada","schemaVersion":1,"correlationId":"trace-123",
                 "payload":{"ventaId":111,"clienteId":"c","fechaCreacion":"2026-11-12T22:00:00Z",
                 "total":4,"items":[{"productoId":%d,"sku":"TRACE-001","cantidad":4,"precioUnitario":1,"subtotal":4}]}}
                """).formatted(producto.id()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        envelopeProcessor.procesar(evento);
        envelopeProcessor.procesar(evento);
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(1);
        assertThat(outboxRepository.findAll()).hasSize(2).allSatisfy(e -> assertThat(e.getPayload()).contains("\"correlationId\":\"trace-123\""));
        assertThat(outboxRepository.findAll()).extracting(e -> e.getEventType())
                .containsExactlyInAnyOrder("MovimientoInventarioRegistrado", "InventarioCritico");
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void evitaStockNegativoAnteDosVentasSimultaneas() throws Exception {
        ProductoResponse producto = crearProducto("CON-001");
        inventarioService.registrarEntrada(
                new OperacionStockRequest(producto.id(), 10, "COMPRA-CONCURRENCIA")
        );

        CountDownLatch inicio = new CountDownLatch(1);
        Callable<Boolean> ventaUno = ventaConcurrente(producto.id(), "VENTA-CON-1", inicio);
        Callable<Boolean> ventaDos = ventaConcurrente(producto.id(), "VENTA-CON-2", inicio);
        Future<Boolean> resultadoUno = executor.submit(ventaUno);
        Future<Boolean> resultadoDos = executor.submit(ventaDos);
        inicio.countDown();

        List<Boolean> resultados = List.of(resultadoUno.get(), resultadoDos.get());
        assertThat(resultados).containsExactlyInAnyOrder(true, false);
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(3);
    }

    @Test
    void repiteReferenciaSinAplicarDosVecesLaEntrada() {
        ProductoResponse producto = crearProducto("IDEM-001");
        OperacionStockRequest request = new OperacionStockRequest(producto.id(), 5, "COMPRA-IDEM-1");

        assertThat(inventarioService.registrarEntrada(request).duplicada()).isFalse();
        assertThat(inventarioService.registrarEntrada(request).duplicada()).isTrue();
        assertThat(inventarioService.consultarStock(producto.id(), true).stockActual()).isEqualTo(5);
        assertThat(movimientoRepository.count()).isEqualTo(1);
    }

    private Callable<Boolean> ventaConcurrente(
            Long productoId,
            String referencia,
            CountDownLatch inicio
    ) {
        return () -> {
            inicio.await();
            try {
                inventarioService.registrarSalida(
                        new OperacionStockRequest(productoId, 7, referencia)
                );
                return true;
            } catch (ConflictoNegocioException exception) {
                return false;
            }
        };
    }

    private ProductoResponse crearProducto(String sku) {
        return productoService.crear(new ProductoCreateRequest(
                sku,
                "Producto concurrente",
                null,
                "Pruebas",
                new BigDecimal("1000.00"),
                2,
                null
        ));
    }
}
