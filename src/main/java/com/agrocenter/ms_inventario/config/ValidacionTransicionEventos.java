package com.agrocenter.ms_inventario.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/** Impide activar ambos transportes para un mismo flujo de negocio. */
@Configuration
public class ValidacionTransicionEventos {
    @Bean
    static BeanFactoryPostProcessor validarTransicionEventos(Environment env) {
        return factory -> validar(env);
    }

    static void validar(Environment env) {
        boolean publicadorKinesis = env.getProperty("agrocenter.streaming.enabled", Boolean.class, false);
        boolean publicadorKafka = env.getProperty("KAFKA_PUBLISHER_ENABLED", Boolean.class, true);
        boolean consumidorKinesis = env.getProperty("agrocenter.kinesis.consumer.enabled", Boolean.class, false);
        boolean consumidorKafka = env.getProperty("KAFKA_CONSUMER_ENABLED", Boolean.class,
                env.getProperty("spring.kafka.listener.auto-startup", Boolean.class, true));
        if (publicadorKinesis && publicadorKafka) throw new IllegalStateException("No activar publicadores Kafka y Kinesis simultaneamente en inventario");
        if (consumidorKinesis && consumidorKafka) throw new IllegalStateException("No activar consumidores Kafka y Kinesis simultaneamente en inventario");
    }
}
