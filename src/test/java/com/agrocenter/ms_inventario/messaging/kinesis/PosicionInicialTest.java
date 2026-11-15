package com.agrocenter.ms_inventario.messaging.kinesis;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import org.springframework.test.util.ReflectionTestUtils;
import static org.mockito.Mockito.mock;

class PosicionInicialTest {
    @Test void aceptaSoloPosicionesExplicitas() {
        assertThat(KinesisConsumer.validarPosicionInicial("TRIM_HORIZON")).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
        assertThat(KinesisConsumer.validarPosicionInicial("LATEST")).isEqualTo(InitialPositionInStream.LATEST);
        assertThatThrownBy(() -> KinesisConsumer.validarPosicionInicial("AT_TIMESTAMP")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void rechazaValorInvalidoAntesDeIniciarClientesAws() {
        var consumer = new KinesisConsumer(mock(ProcesadorConReintentos.class));
        ReflectionTestUtils.setField(consumer, "initialPosition", "invalido");
        assertThatThrownBy(consumer::start).isInstanceOf(IllegalArgumentException.class);
    }
}
