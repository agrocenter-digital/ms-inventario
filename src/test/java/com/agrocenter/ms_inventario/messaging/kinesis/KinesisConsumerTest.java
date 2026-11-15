package com.agrocenter.ms_inventario.messaging.kinesis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class KinesisConsumerTest {
    @Test void saludDetectaRetrasoYShardPerdido() {
        var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
        ReflectionTestUtils.setField(consumer, "worker", Thread.currentThread());
        var processor = consumer.new Processor();
        processor.initialize(InitializationInput.builder().shardId("shard-1").build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("UNKNOWN");
        processor.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("UP");
        processor.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(3600001L).build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("DOWN");
        processor.leaseLost(LeaseLostInput.builder().build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("UNKNOWN");
    }
    @Test void saludNoOcultaFalloDeOtroShardYSenalaSilencio() {
        var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
        ReflectionTestUtils.setField(consumer, "worker", Thread.currentThread());
        var a = consumer.new Processor();
        var b = consumer.new Processor();
        a.initialize(InitializationInput.builder().shardId("a").build());
        b.initialize(InitializationInput.builder().shardId("b").build());
        b.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(3600001L).build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("DOWN");
        b.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        ReflectionTestUtils.setField(consumer, "maxSilenceMs", -1L);
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("DOWN");
    }
    @Test void consumidorNoIniciadoReportaNoDisponible() {
        var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("DOWN");
    }
    @Test void confirmaSoloTrasProcesar() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var processor = consumer.new Processor();
        processor.processRecords(input(checkpoint));
        verify(handler).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        verify(checkpoint).checkpoint();
    }

    @Test void eventoInvalidoGuardadoEnCuarentenaPermiteCheckpointDelLote() throws Exception {
        var envelope = mock(EnvelopeProcessor.class);
        var cuarentena = mock(CuarentenaService.class);
        doThrow(new EventoInvalidoException("invalido")).when(envelope).procesar(any());
        var consumer = new KinesisConsumer(new ProcesadorConReintentos(envelope, cuarentena));
        ReflectionTestUtils.setField(consumer, "stream", "negocio");
        var processor = consumer.new Processor();
        processor.initialize(InitializationInput.builder().shardId("shard-1").build());
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(0L)
                .data(ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8))).build();
        processor.processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
        var orden = inOrder(cuarentena, checkpoint);
        orden.verify(cuarentena).guardar(eq("negocio"), eq("shard-1"), eq("123"), eq(0L), any(), any(), eq(1));
        orden.verify(checkpoint).checkpoint();
    }

    @Test void transmiteSubsecuenciaDelRegistro() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(7L)
                .data(ByteBuffer.wrap(new byte[] {1})).build();
        consumer.new Processor().processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
        verify(handler).procesar(nullable(String.class), nullable(String.class), eq("123"), eq(7L), any(byte[].class));
        verify(checkpoint).checkpoint();
    }

    @Test void falloSeRecuperaSinConfirmarAntesDelExito() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        ReflectionTestUtils.setField(consumer, "retryDelayMs", 100L);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doAnswer(invocation -> { verify(checkpoint, never()).checkpoint(); throw new IllegalStateException("simulado"); })
                .doNothing().when(handler).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        var processor = consumer.new Processor();
        processor.processRecords(input(checkpoint));
        verify(handler, times(2)).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        verify(checkpoint).checkpoint();
    }

    @Test void saludEstaDownMientrasUnShardReintenta() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        ReflectionTestUtils.setField(consumer, "worker", Thread.currentThread());
        ReflectionTestUtils.setField(consumer, "retryDelayMs", 100L);
        var processor = consumer.new Processor();
        processor.initialize(InitializationInput.builder().shardId("a").build());
        processor.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new IllegalStateException("temporal"))
                .doAnswer(invocation -> { assertThat(consumer.health().getStatus().getCode()).isEqualTo("DOWN"); return null; })
                .when(handler).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        processor.processRecords(ProcessRecordsInput.builder().records(input(checkpoint).records())
                .checkpointer(checkpoint).millisBehindLatest(0L).build());
        assertThat(consumer.health().getStatus().getCode()).isEqualTo("UP");
    }

    @Test void cierreDuranteFalloNoConfirma() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        ReflectionTestUtils.setField(consumer, "retryDelayMs", 100L);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doAnswer(invocation -> {
            ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(consumer, "stopping")).set(true);
            throw new IllegalStateException("fallo");
        }).when(handler).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        consumer.new Processor().processRecords(input(checkpoint));
        verify(checkpoint, never()).checkpoint();
    }

    @Test void falloDeCheckpointDetieneElConsumidor() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        var scheduler = mock(Scheduler.class);
        ReflectionTestUtils.setField(consumer, "scheduler", scheduler);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new IllegalStateException("checkpoint")).when(checkpoint).checkpoint();
        var processor = consumer.new Processor();
        processor.processRecords(input(checkpoint));
        processor.processRecords(input(checkpoint));
        verify(handler, times(1)).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
        verify(scheduler).startGracefulShutdown();
    }

    @Test void checkpointTemporalSeReintentaSinRepetirElEfecto() throws Exception {
        var handler = mock(ProcesadorConReintentos.class);
        var consumer = new KinesisConsumer(handler);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("temporal")).doNothing().when(checkpoint).checkpoint();
        consumer.new Processor().processRecords(input(checkpoint));
        verify(checkpoint, times(2)).checkpoint();
        verify(handler, times(1)).procesar(nullable(String.class), nullable(String.class), nullable(String.class), anyLong(), any(byte[].class));
    }

    @Test void checkpointTemporalAgotadoDetieneYShutdownNoSeReintenta() throws Exception {
        for (boolean shutdown : new boolean[] {false, true}) {
            var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
            var scheduler = mock(Scheduler.class);
            ReflectionTestUtils.setField(consumer, "scheduler", scheduler);
            var checkpoint = mock(RecordProcessorCheckpointer.class);
            if (shutdown) doThrow(new ShutdownException("lease perdido")).when(checkpoint).checkpoint();
            else doThrow(new ThrottlingException("temporal")).when(checkpoint).checkpoint();
            consumer.new Processor().processRecords(input(checkpoint));
            verify(checkpoint, times(shutdown ? 1 : 3)).checkpoint();
            verify(scheduler).startGracefulShutdown();
        }
    }

    @Test void reintentaCheckpointAlFinalizarShard() throws Exception {
        var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("temporal")).doNothing().when(checkpoint).checkpoint();
        consumer.new Processor().shardEnded(ShardEndedInput.builder().checkpointer(checkpoint).build());
        verify(checkpoint, times(2)).checkpoint();
    }

    private ProcessRecordsInput input(RecordProcessorCheckpointer checkpoint) {
        var record = KinesisClientRecord.builder().data(ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8))).build();
        return ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build();
    }
}
