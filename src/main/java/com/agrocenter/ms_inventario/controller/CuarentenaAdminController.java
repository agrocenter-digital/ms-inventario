package com.agrocenter.ms_inventario.controller;

import java.time.Instant;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Solo metadatos: no carga ni devuelve el contenido original de cuarentena. */
@RestController
@RequestMapping("/api/inventario/admin/cuarentena")
@PreAuthorize("hasRole('ADMIN')")
public class CuarentenaAdminController {
    private final JdbcTemplate jdbc;
    public CuarentenaAdminController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping
    public ResponseEntity<Pagina> listar(@RequestParam(defaultValue = "0") int pagina,
                                        @RequestParam(defaultValue = "25") int tamano,
                                        @RequestParam(defaultValue = "true") boolean pendientes) {
        if (pagina < 0 || pagina > 10000 || tamano < 1 || tamano > 100) return ResponseEntity.badRequest().build();
        String filtro = pendientes ? " WHERE resuelto_en IS NULL" : "";
        List<Metadata> items = jdbc.query("SELECT id, stream, shard, sequence_number, sub_sequence_number, error_tipo, intentos, created_at, resuelto_en FROM eventos_kinesis_cuarentena"
                + filtro + " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?", (rs, n) -> new Metadata(
                rs.getString("id"), rs.getString("stream"), rs.getString("shard"), rs.getString("sequence_number"),
                rs.getLong("sub_sequence_number"), rs.getString("error_tipo"), rs.getInt("intentos"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("resuelto_en") == null ? null : rs.getTimestamp("resuelto_en").toInstant()),
                tamano + 1, (long) pagina * tamano);
        return ResponseEntity.ok(new Pagina(List.copyOf(items.subList(0, Math.min(items.size(), tamano))), pagina, tamano, items.size() > tamano));
    }

    public record Metadata(String id, String stream, String shard, String sequenceNumber, long subSequenceNumber,
                           String tipoError, int intentos, Instant createdAt, Instant resueltoEn) { }
    public record Pagina(List<Metadata> elementos, int pagina, int tamano, boolean hayMas) { }
}
