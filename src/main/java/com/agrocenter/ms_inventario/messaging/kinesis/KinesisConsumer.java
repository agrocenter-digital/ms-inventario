package com.agrocenter.ms_inventario.messaging.kinesis;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "agrocenter.kinesis.consumer.enabled", havingValue = "true")
public class KinesisConsumer implements HealthIndicator {
    private final ProcesadorConReintentos handler;
    @Value("${aws.region:us-east-1}") private String regionName;
    @Value("${agrocenter.kinesis.consumer.stream:agrocenter-kinesis-negocio}") private String stream;
    @Value("${agrocenter.kinesis.consumer.application:agrocenter-inventario-negocio}") private String application;
    @Value("${KINESIS_INITIAL_POSITION:TRIM_HORIZON}") private String initialPosition;
    private KinesisAsyncClient kinesis;
    private DynamoDbAsyncClient dynamo;
    private CloudWatchAsyncClient cloudWatch;
    private Scheduler scheduler;
    private Thread worker;
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicBoolean fatal = new AtomicBoolean();
    private final ConcurrentHashMap<String, ShardStatus> shards = new ConcurrentHashMap<>();
    private final AtomicBoolean stopping = new AtomicBoolean();
    @Value("${agrocenter.kinesis.consumer.retry-delay-ms:5000}") private long retryDelayMs = 5000;
    @Value("${agrocenter.kinesis.consumer.max-lag-ms:3600000}") private long maxLagMs = 3600000;
    @Value("${agrocenter.kinesis.consumer.max-silence-ms:120000}") private long maxSilenceMs = 120000;

    private static final class ShardStatus {
        final long assignedAt = System.nanoTime();
        volatile long observedAt;
        volatile Long lagMs;
        volatile boolean retrying;
    }

    @Override public Health health() {
        if (failed.get() || stopping.get() || worker == null || !worker.isAlive()) return Health.down().withDetail("consumer", "detenido").build();
        if (shards.isEmpty()) return Health.unknown().withDetail("consumer", "sin shard asignado").build();
        long now = System.nanoTime();
        boolean sinDato = false;
        for (ShardStatus status : shards.values()) {
            if (status.retrying) return Health.down().withDetail("consumer", "reintentando").build();
            long last = status.observedAt == 0 ? status.assignedAt : status.observedAt;
            if (TimeUnit.NANOSECONDS.toMillis(now - last) > maxSilenceMs)
                return Health.down().withDetail("consumer", "sin lecturas recientes").build();
            if (status.lagMs == null) sinDato = true;
            else if (status.lagMs > maxLagMs) return Health.down().withDetail("consumer", "retraso elevado").withDetail("lagMs", status.lagMs).build();
        }
        if (sinDato) return Health.unknown().withDetail("consumer", "sin dato de retraso").build();
        return Health.up().withDetail("consumer", "lecturas recientes").withDetail("shards", shards.size()).build();
    }

    @PostConstruct
    public void start() {
        var posicion = validarPosicionInicial(initialPosition);
        Region region = Region.of(regionName);
        kinesis = KinesisAsyncClient.builder().region(region).build();
        dynamo = DynamoDbAsyncClient.builder().region(region).build();
        cloudWatch = CloudWatchAsyncClient.builder().region(region).build();
        ConfigsBuilder c = new ConfigsBuilder(stream, application, kinesis, dynamo, cloudWatch,
                UUID.randomUUID().toString(), Processor::new);
        scheduler = new Scheduler(c.checkpointConfig(), c.coordinatorConfig(), c.leaseManagementConfig(),
                c.lifecycleConfig(), c.metricsConfig().metricsLevel(MetricsLevel.NONE), c.processorConfig().callProcessRecordsEvenForEmptyRecordList(true),
                c.retrievalConfig().retrievalSpecificConfig(new PollingConfig(stream, kinesis))
                        .initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(posicion)));
        worker = new Thread(scheduler, "kcl-inventario");
        worker.start();
    }

    static InitialPositionInStream validarPosicionInicial(String valor) {
        if ("TRIM_HORIZON".equals(valor)) return InitialPositionInStream.TRIM_HORIZON;
        if ("LATEST".equals(valor)) return InitialPositionInStream.LATEST;
        throw new IllegalArgumentException("KINESIS_INITIAL_POSITION debe ser TRIM_HORIZON o LATEST");
    }

    @PreDestroy
    public void stop() {
        stopping.set(true);
        try {
            if (scheduler != null) scheduler.startGracefulShutdown().get(20, TimeUnit.SECONDS);
        } catch (Exception e) { log.error("No se pudo detener KCL limpiamente", e); }
        finally {
            if (kinesis != null) kinesis.close();
            if (dynamo != null) dynamo.close();
            if (cloudWatch != null) cloudWatch.close();
        }
    }

    final class Processor implements ShardRecordProcessor {
        private String shard;
        private final ShardStatus status = new ShardStatus();
        @Override public void initialize(InitializationInput input) { shard = input.shardId(); shards.put(shard, status); log.info("Shard asignado {}", shard); }
        @Override public void processRecords(ProcessRecordsInput input) {
            if (fatal.get() || stopping.get()) return;
            try {
                for (var record : input.records()) {
                    var buffer = record.data().asReadOnlyBuffer();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    while (!stopping.get()) {
                        try {
                            handler.procesar(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes);
                            status.retrying = false;
                            break;
                        } catch (RuntimeException e) {
                            status.retrying = true;
                            log.error("Fallo al procesar registro Kinesis; se reintentara sin avanzar checkpoint", e);
                            Thread.sleep(Math.max(100, retryDelayMs));
                        }
                    }
                    if (stopping.get()) return;
                }
                if (!input.records().isEmpty() && !confirmar(input.checkpointer())) return;
                status.lagMs = input.millisBehindLatest();
                status.observedAt = System.nanoTime();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed.set(true);
                fatal.set(true);
                scheduler.startGracefulShutdown();
            } catch (Exception e) {
                failed.set(true);
                fatal.set(true);
                log.error("Fallo de consumo Kinesis: se detiene sin avanzar checkpoint; requiere intervencion", e);
                scheduler.startGracefulShutdown();
            }
        }
        @Override public void leaseLost(LeaseLostInput input) { if (shard != null) shards.remove(shard, status); log.warn("Lease Kinesis perdido"); }
        private boolean confirmar(RecordProcessorCheckpointer checkpointer) throws Exception {
            for (int intento = 1; intento <= 3 && !stopping.get() && !fatal.get(); intento++) {
                try {
                    checkpointer.checkpoint();
                    status.retrying = false;
                    return true;
                } catch (KinesisClientLibRetryableException e) {
                    status.retrying = true;
                    if (intento == 3) throw e;
                    log.warn("Checkpoint temporalmente fallido shard={} intento={} tipo={}", shard, intento, e.getClass().getSimpleName());
                    Thread.sleep(200L * intento);
                }
            }
            return false;
        }
        @Override public void shardEnded(ShardEndedInput input) {
            if (fatal.get() || stopping.get()) return;
            try { if (confirmar(input.checkpointer()) && shard != null) shards.remove(shard, status); }
            catch (Exception e) { if (e instanceof InterruptedException) Thread.currentThread().interrupt(); failed.set(true); fatal.set(true); scheduler.startGracefulShutdown(); log.error("Checkpoint de shard fallido", e); }
        }
        @Override public void shutdownRequested(ShutdownRequestedInput input) { /* No avanzar sin procesar. */ }
    }
}
