package com.agrocenter.ms_inventario.messaging.kinesis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcesadorConReintentos {
    private final EnvelopeProcessor handler;
    private final CuarentenaService cuarentena;

    public void procesar(String stream, String shard, String sequence, long subSequence, byte[] bytes) {
        for (int intento = 1; intento <= 3; intento++) {
            try {
                handler.procesar(bytes);
                return;
            } catch (EventoInvalidoException e) {
                cuarentena.guardar(stream, shard, sequence, subSequence, bytes, e, intento);
                log.warn("Evento invalido en cuarentena stream={} shard={} sequence={} subsequence={}", stream, shard, sequence, subSequence);
                return;
            } catch (RuntimeException e) {
                if (intento == 3) throw e;
                log.warn("Error transitorio al procesar stream={} shard={} intento={} tipo={}", stream, shard, intento, e.getClass().getSimpleName());
                try { Thread.sleep(200L * (1L << (intento - 1))); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Reintento interrumpido", interrupted);
                }
            }
        }
    }
}
