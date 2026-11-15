package com.agrocenter.ms_inventario.messaging.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

@ExtendWith(MockitoExtension.class)
class KinesisOutboxPublisherTest {
    @Mock OutboxRepository repository;
    @Mock KinesisClient client;
    KinesisOutboxPublisher publisher;
    OutboxEvent event;

    @BeforeEach void preparar() {
        publisher = new KinesisOutboxPublisher(repository, client);
        ReflectionTestUtils.setField(publisher, "stream", "agrocenter-kinesis-stock");
        event = new OutboxEvent("id-1", "InventarioCritico", "12", "producto:12", "{\"eventId\":\"id-1\"}", Instant.now());
        when(repository.buscarPublicables(any(), any()))
                .thenReturn(List.of(event));
    }

    @Test void publicaEnStreamDeStock() {
        when(client.putRecord(any(PutRecordRequest.class))).thenReturn(PutRecordResponse.builder().build());
        publisher.publicarUno();
        var request = ArgumentCaptor.forClass(PutRecordRequest.class);
        verify(client).putRecord(request.capture());
        assertThat(request.getValue().streamName()).isEqualTo("agrocenter-kinesis-stock");
        assertThat(request.getValue().partitionKey()).isEqualTo("producto:12");
        assertThat(event.getSentAt()).isNotNull();
    }

    @Test void falloQuedaPendiente() {
        when(client.putRecord(any(PutRecordRequest.class))).thenThrow(new IllegalStateException("sin conexion"));
        publisher.publicarUno();
        assertThat(event.getSentAt()).isNull();
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isAfter(event.getCreatedAt());
    }
}
