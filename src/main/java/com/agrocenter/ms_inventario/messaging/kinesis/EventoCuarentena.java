package com.agrocenter.ms_inventario.messaging.kinesis;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Getter;

@Entity
@Table(name = "eventos_kinesis_cuarentena")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class EventoCuarentena {
    @Id @Column(length = 64) private String id;
    @Column(nullable = false, length = 128) private String stream;
    @Column(nullable = false, length = 128) private String shard;
    @Column(name = "sequence_number", nullable = false, length = 128) private String sequenceNumber;
    @Column(name = "sub_sequence_number", nullable = false) private long subSequenceNumber;
    @Column(name = "payload_base64", nullable = false, columnDefinition = "TEXT") private String payloadBase64;
    @Column(name = "error_tipo", nullable = false, length = 200) private String errorTipo;
    @Column(nullable = false) private int intentos;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "resuelto_en") private Instant resueltoEn;

    public void marcarResuelto() { this.resueltoEn = Instant.now(); }

    public EventoCuarentena(String id, String stream, String shard, String sequenceNumber, long subSequenceNumber, String payloadBase64, String errorTipo, int intentos) {
        this.id = id; this.stream = stream; this.shard = shard; this.sequenceNumber = sequenceNumber;
        this.subSequenceNumber = subSequenceNumber; this.payloadBase64 = payloadBase64; this.errorTipo = errorTipo; this.intentos = intentos; this.createdAt = Instant.now();
    }
}
