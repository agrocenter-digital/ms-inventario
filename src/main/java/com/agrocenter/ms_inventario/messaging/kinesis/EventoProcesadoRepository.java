package com.agrocenter.ms_inventario.messaging.kinesis;

import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoProcesadoRepository extends JpaRepository<EventoProcesado, String> { }
