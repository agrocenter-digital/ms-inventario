package com.agrocenter.ms_inventario.integration;

import com.agrocenter.ms_inventario.repository.MovimientoInventarioRepository;
import com.agrocenter.ms_inventario.repository.ProductoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class SecurityIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MovimientoInventarioRepository movimientoRepository;

    @Autowired
    private ProductoRepository productoRepository;

    @BeforeEach
    void limpiarDatos() {
        movimientoRepository.deleteAll();
        productoRepository.deleteAll();
    }

    @Test
    void permiteHealthCheckSinJwt() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    void noExponeEmisorLocalFueraDelPerfilDev() throws Exception {
        mockMvc.perform(post("/api/dev/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"usuario\":\"intruso\",\"rol\":\"ADMIN\"}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void rechazaAccesoSinJwt() throws Exception {
        mockMvc.perform(get("/api/inventario/productos"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error").value("UNAUTHORIZED"));
    }

    @Test
    void clientePuedeConsultarCatalogo() throws Exception {
        mockMvc.perform(get("/api/inventario/productos")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE"))))
                .andExpect(status().isOk());
    }

    @Test
    void clienteNoPuedeCrearProductos() throws Exception {
        mockMvc.perform(post("/api/inventario/productos")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productoJson("SEG-CLIENTE")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("FORBIDDEN"));
    }

    @Test
    void adminPuedeCrearProductos() throws Exception {
        mockMvc.perform(post("/api/inventario/productos")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productoJson("SEG-ADMIN")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.sku").value("SEG-ADMIN"));
    }

    private String productoJson(String sku) {
        return """
                {
                  "sku": "%s",
                  "nombre": "Producto de seguridad",
                  "descripcion": "Prueba de autorizacion",
                  "categoria": "Pruebas",
                  "precioVenta": 1000.00,
                  "stockMinimo": 2
                }
                """.formatted(sku);
    }
}
