package com.agrocenter.ms_inventario.service;
import com.agrocenter.ms_inventario.entity.Producto;
import com.agrocenter.ms_inventario.mapper.InventarioMapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
class CatalogoModalidadTest {
    @Test
    void clasificaSoloSkusExplicitosSinInferirPorCategoria() {
        InventarioMapper mapper = new InventarioMapper();
        Producto producto = new Producto("EQ-1", "Equipo", "Descripción", "Maquinaria", BigDecimal.TEN, 0);
        assertThat(mapper.toProductoResponse(producto).soloCotizacion()).isFalse();
        ReflectionTestUtils.setField(mapper, "skusSoloCotizacion", " OTRO, eq-1 ");
        assertThat(mapper.toProductoResponse(producto).soloCotizacion()).isTrue();
        ReflectionTestUtils.setField(mapper, "skusSoloCotizacion", "EQ-10");
        assertThat(mapper.toProductoResponse(producto).soloCotizacion()).isFalse();
    }
}
