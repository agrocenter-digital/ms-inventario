package com.agrocenter.ms_inventario.integration;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = {"KAFKA_CONSUMER_ENABLED=false", "spring.kafka.listener.auto-startup=true", "spring.kafka.admin.auto-create=false"})
@ActiveProfiles("test")
class KafkaConsumerSwitchTest {
    @Autowired KafkaListenerEndpointRegistry registry;
    @Test void banderaExplicitaImpideArranqueAunqueDefaultGlobalSeaTrue() {
        assertThat(registry.getListenerContainers()).isNotEmpty().allSatisfy(c -> assertThat(c.isRunning()).isFalse());
    }
}
