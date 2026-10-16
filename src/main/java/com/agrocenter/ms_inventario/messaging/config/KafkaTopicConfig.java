package com.agrocenter.ms_inventario.messaging.config;

import com.agrocenter.ms_inventario.messaging.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic ordenesComprasTopic() {
        return TopicBuilder.name(KafkaTopics.ORDENES_COMPRAS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic inventarioCriticoTopic() {
        return TopicBuilder.name(KafkaTopics.INVENTARIO_CRITICO)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
