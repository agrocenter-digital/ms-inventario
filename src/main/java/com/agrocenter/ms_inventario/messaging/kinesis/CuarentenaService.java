package com.agrocenter.ms_inventario.messaging.kinesis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CuarentenaService {
    private final CuarentenaRepository repository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void guardar(String stream, String shard, String sequence, long subSequence, byte[] payload, Exception error, int attempts) {
        if (stream == null || shard == null || sequence == null || subSequence < 0) throw new IllegalArgumentException("Metadatos incompletos");
        String key = stream + "\n" + shard + "\n" + sequence;
        // Compatibilidad con registros previos a esta migración, que asumían subsecuencia cero.
        if (subSequence != 0) key += "\n" + subSequence;
        try {
            String id = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));
            if (!repository.existsById(id)) repository.saveAndFlush(new EventoCuarentena(id, stream, shard, sequence, subSequence,
                    Base64.getEncoder().encodeToString(payload), error.getClass().getSimpleName(), attempts));
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
