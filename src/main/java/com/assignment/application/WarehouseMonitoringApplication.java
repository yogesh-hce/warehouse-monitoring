package com.assignment.application;

import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.assignment.config.KafkaConsumerConfig;
import com.assignment.config.KafkaProducerConfig;
import com.assignment.service.CentralMonitoringService;
import com.assignment.service.WarehouseService;

public class WarehouseMonitoringApplication {
	
    private static final Logger logger = LoggerFactory.getLogger(WarehouseMonitoringApplication.class);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"; // Actual kafka bootstrap URLs, should be read from property file
    private static final String CONSUMER_GROUP_ID = "monitoring-consumer-group";
    private static final String OFFSET_RESET = "earliest";
    private static final String ACKS_CONFIG = "all";
    private static final int UDP_PORT = 3344;
    
    public static void main(String[] args) {
        // Create Kafka Producer and Consumer using configuration classes
        KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerConfig.createKafkaConsumer(
            KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID, OFFSET_RESET
        );
        KafkaProducer<String, String> kafkaProducer = KafkaProducerConfig.createKafkaProducer(
            KAFKA_BOOTSTRAP_SERVERS, ACKS_CONFIG
        );

        // Initialize DatagramSocket for UDP communication
        DatagramSocket datagramSocket;
        try {
            datagramSocket = new DatagramSocket(UDP_PORT);
        } catch (Exception e) {
            logger.error("Failed to initialize DatagramSocket on port {}: {}", UDP_PORT, e.getMessage());
            return;
        }

        WarehouseService warehouseService = new WarehouseService(kafkaProducer, datagramSocket, logger);
        CentralMonitoringService centralMonitoringService = new CentralMonitoringService(kafkaConsumer, logger);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Start the services
        executorService.submit(warehouseService::start);
        executorService.submit(centralMonitoringService::start);
        
        logger.info("Application started. WarehouseService and CentralMonitoringService are running.");

        // For graceful exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down services...");
            warehouseService.stop();
            centralMonitoringService.stop();
            executorService.shutdown();
        }));
    }
}
