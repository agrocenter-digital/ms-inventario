package com.agrocenter.ms_inventario.messaging.kinesis;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "eventos_kinesis_procesados")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EventoProcesado {
    @Id @Column(name = "event_id", length = 100) private String eventId;
    @Column(name = "procesado_en", nullable = false) private Instant procesadoEn;

    public EventoProcesado(String eventId) { this.eventId = eventId; this.procesadoEn = Instant.now(); }
}
