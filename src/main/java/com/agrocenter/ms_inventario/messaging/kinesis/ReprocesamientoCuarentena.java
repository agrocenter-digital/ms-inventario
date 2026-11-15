package com.agrocenter.ms_inventario.messaging.kinesis;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Operación interna: no exponer sin autenticación y autorización administrativa. */
@Service
@RequiredArgsConstructor
public class ReprocesamientoCuarentena {
    private final CuarentenaRepository repository;
    private final EnvelopeProcessor processor;

    public enum Resultado { APLICADO, YA_RESUELTO }

    /** YA_RESUELTO no implica que el nuevo JSON coincida con la corrección anterior. */
    @Transactional
    public Resultado reprocesar(String id, byte[] payloadCorregido) {
        EventoCuarentena item = repository.buscarParaReprocesar(id)
                .orElseThrow(() -> new IllegalArgumentException("Evento en cuarentena no encontrado"));
        if (item.getResueltoEn() != null) return Resultado.YA_RESUELTO;
        if (payloadCorregido == null || payloadCorregido.length == 0)
            throw new IllegalArgumentException("Se requiere un payload corregido");
        if (!processor.procesar(payloadCorregido))
            throw new EventoInvalidoException("El evento corregido no corresponde a este consumidor");
        item.marcarResuelto();
        repository.saveAndFlush(item);
        return Resultado.APLICADO;
    }
}
