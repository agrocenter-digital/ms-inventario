package com.agrocenter.ms_inventario.integration;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.agrocenter.ms_inventario.messaging.kinesis.CuarentenaRepository;
import com.agrocenter.ms_inventario.messaging.kinesis.CuarentenaService;
import com.agrocenter.ms_inventario.messaging.kinesis.EventoInvalidoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class CuarentenaAdminIntegrationTest {
    @Autowired MockMvc mvc;
    @Autowired CuarentenaRepository repository;
    @Autowired CuarentenaService cuarentena;

    @BeforeEach void preparar() {
        repository.deleteAll();
        cuarentena.guardar("negocio", "shard-1", "1", 0, "contenido-privado".getBytes(), new EventoInvalidoException("x"), 1);
        cuarentena.guardar("negocio", "shard-1", "2", 0, "contenido-privado".getBytes(), new EventoInvalidoException("x"), 1);
        var resuelto = repository.findAll().getFirst();
        resuelto.marcarResuelto(); repository.saveAndFlush(resuelto);
    }

    @Test void soloAdminConsultaMetadatosConPaginacionYFiltro() throws Exception {
        String path = "/api/inventario/admin/cuarentena";
        mvc.perform(get(path)).andExpect(status().isUnauthorized());
        mvc.perform(get(path).with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE")))).andExpect(status().isForbidden());
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get(path).with(admin)).andExpect(status().isOk()).andExpect(jsonPath("$.elementos.length()").value(1))
                .andExpect(jsonPath("$.elementos[0].stream").value("negocio"))
                .andExpect(jsonPath("$.elementos[0].payloadBase64").doesNotExist())
                .andExpect(jsonPath("$.elementos[0].resueltoEn").doesNotExist());
        mvc.perform(get(path).param("pendientes", "false").param("tamano", "1").with(admin))
                .andExpect(status().isOk()).andExpect(jsonPath("$.hayMas").value(true));
        mvc.perform(get(path).param("tamano", "101").with(admin)).andExpect(status().isBadRequest());
    }
}
