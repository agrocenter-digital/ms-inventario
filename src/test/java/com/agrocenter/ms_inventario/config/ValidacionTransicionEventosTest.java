package com.agrocenter.ms_inventario.config;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class ValidacionTransicionEventosTest {
    @Test void rechazaDosPublicadoresYDosConsumidores() {
        assertThatCode(() -> ValidacionTransicionEventos.validar(new MockEnvironment())).doesNotThrowAnyException();
        var env = new MockEnvironment().withProperty("agrocenter.streaming.enabled", "true");
        assertThatThrownBy(() -> ValidacionTransicionEventos.validar(env)).isInstanceOf(IllegalStateException.class);
        env.setProperty("KAFKA_PUBLISHER_ENABLED", "false");
        env.setProperty("agrocenter.kinesis.consumer.enabled", "true");
        assertThatThrownBy(() -> ValidacionTransicionEventos.validar(env)).isInstanceOf(IllegalStateException.class);
        env.setProperty("KAFKA_CONSUMER_ENABLED", "false");
        assertThatCode(() -> ValidacionTransicionEventos.validar(env)).doesNotThrowAnyException();
    }
    @Test void respetaElValorGlobalSiNoExisteBanderaExplicita() {
        var env = new MockEnvironment().withProperty("agrocenter.kinesis.consumer.enabled", "true")
                .withProperty("spring.kafka.listener.auto-startup", "false");
        assertThatCode(() -> ValidacionTransicionEventos.validar(env)).doesNotThrowAnyException();
        env.setProperty("KAFKA_CONSUMER_ENABLED", "true");
        assertThatThrownBy(() -> ValidacionTransicionEventos.validar(env)).isInstanceOf(IllegalStateException.class);
    }
}
