package com.assignment.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class WarehouseServiceIT {

    private static final String TOPIC = "sensor-data";
    private static final String VALID_DATA = "sensor_id=t1;value=30.0";
    private static final int TEMP_PORT = 3344;

    private KafkaContainer kafkaContainer;
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;
    private WarehouseService warehouseService;
    private Logger logger;
    private DatagramSocket datagramSocket;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.start();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

        logger = mock(Logger.class);
        datagramSocket = new DatagramSocket(TEMP_PORT);

        warehouseService = new WarehouseService(kafkaProducer, datagramSocket, logger);
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> warehouseService.start());
    }

    @Test
    public void testDataReceptionAndKafkaSend() throws Exception {
        sendUDPData(VALID_DATA);
        
        ConsumerRecord<String, String> record = kafkaConsumer.poll(Duration.ofSeconds(10)).iterator().next();
        assertEquals(VALID_DATA, record.value());

        verify(logger).info("Received data: {}", VALID_DATA);
        verify(logger).info("Sent data to Kafka: {}", VALID_DATA);
    }


    @AfterEach
    public void tearDown() throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        if (kafkaProducer != null) kafkaProducer.close();
        if (kafkaConsumer != null) kafkaConsumer.close();
        if (datagramSocket != null) datagramSocket.close();
        if (kafkaContainer != null) kafkaContainer.stop();
    }

    private void sendUDPData(String data) throws Exception {
        byte[] buffer = data.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("localhost"), TEMP_PORT);
        DatagramSocket senderSocket = new DatagramSocket();
        senderSocket.send(packet);
        senderSocket.close();
    }
}