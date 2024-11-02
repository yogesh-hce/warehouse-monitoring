package com.assignment.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

    /**
     * Creates a Kafka producer with the specified configuration.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param acksConfig Acknowledgment setting
     * @return Configured KafkaProducer instance
     */
    public static KafkaProducer<String, String> createKafkaProducer(String bootstrapServers, String acksConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acksConfig != null ? acksConfig : "all");

        try {
            return new KafkaProducer<>(props);
        } catch (Exception e) {
            logger.error("Failed to create Kafka producer with properties: {}", props, e);
            throw new RuntimeException("Failed to create Kafka producer", e);
        }
    }
}