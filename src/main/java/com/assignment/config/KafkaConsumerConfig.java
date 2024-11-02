package com.assignment.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /**
     * Creates a Kafka consumer with the specified configuration.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId Consumer group ID
     * @param offsetReset Auto offset reset configuration
     * @return Configured KafkaConsumer instance
     */
    public static KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServers, 
                                                                    String groupId,
                                                                    String offsetReset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null ? groupId : "monitoring-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset != null ? offsetReset : "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try {
            return new KafkaConsumer<>(props);
        } catch (Exception e) {
            logger.error("Failed to create Kafka consumer with properties: {}", props, e);
            throw new RuntimeException("Failed to create Kafka consumer", e);
        }
    }
}