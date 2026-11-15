package com.agrocenter.ms_inventario.messaging.kinesis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.Test;

class ProcesadorConReintentosTest {
    @Test void transitorioSeRecuperaEnTercerIntento() {
        var handler = mock(EnvelopeProcessor.class);
        var quarantine = mock(CuarentenaService.class);
        doThrow(new IllegalStateException()).doThrow(new IllegalStateException()).doReturn(true).when(handler).procesar(any());
        new ProcesadorConReintentos(handler, quarantine).procesar("s", "sh", "1", 0L, new byte[0]);
        verify(handler, times(3)).procesar(any());
        verifyNoInteractions(quarantine);
    }
    @Test void eventoInvalidoSeGuardaYFalloDeCuarentenaSePropaga() {
        var handler = mock(EnvelopeProcessor.class);
        var quarantine = mock(CuarentenaService.class);
        doThrow(new EventoInvalidoException("invalido")).when(handler).procesar(any());
        var processor = new ProcesadorConReintentos(handler, quarantine);
        processor.procesar("s", "sh", "1", 7L, new byte[0]);
        verify(quarantine).guardar(eq("s"), eq("sh"), eq("1"), eq(7L), any(), any(), eq(1));
        doThrow(new IllegalStateException("db caida")).when(quarantine).guardar(any(), any(), any(), anyLong(), any(), any(), anyInt());
        assertThatThrownBy(() -> processor.procesar("s", "sh", "2", 0L, new byte[0])).isInstanceOf(IllegalStateException.class);
    }
    @Test void errorTransitorioAgotadoNoSeDescarta() {
        var handler = mock(EnvelopeProcessor.class);
        var quarantine = mock(CuarentenaService.class);
        doThrow(new IllegalStateException("db caida")).when(handler).procesar(any());
        assertThatThrownBy(() -> new ProcesadorConReintentos(handler, quarantine).procesar("s", "sh", "1", 0L, new byte[0]))
                .isInstanceOf(IllegalStateException.class);
        verify(handler, times(3)).procesar(any());
        verifyNoInteractions(quarantine);
    }
}
