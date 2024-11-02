package com.assignment.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class CentralMonitoringServiceIT {

    private static final String TOPIC = "sensor-data";
    private static final String TEMPERATURE_EXCEEDING_DATA = "sensor_id=t1;value=40";
    private static final String TEMPERATURE_NORMAL_DATA = "sensor_id=t1;value=30";
    private static final String HUMIDITY_EXCEEDING_DATA = "sensor_id=h1;value=55";
    private static final String HUMIDITY_NORMAL_DATA = "sensor_id=h1;value=45";
    private static final String INVALID_DATA = "sensor_id=t1;val=invalid";

    private KafkaContainer kafkaContainer;
    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;
    private CentralMonitoringService centralMonitoringService;
    private Logger logger;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
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

        centralMonitoringService = new CentralMonitoringService(kafkaConsumer, logger);

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> centralMonitoringService.start());
    }

    @Test
    public void testTemperatureExceedingThreshold() {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, TEMPERATURE_EXCEEDING_DATA));
        waitForProcessing();
        verify(logger).warn("ALARM: Temperature threshold exceeded! Sensor ID: {}, Value: {}", "t1", 40.0);
    }

    @Test
    public void testTemperatureWithinThreshold() {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, TEMPERATURE_NORMAL_DATA));
        waitForProcessing();
        verify(logger).info("Data is within safe thresholds. Sensor ID: {}, Value: {}", "t1", 30.0);
    }

    @Test
    public void testHumidityExceedingThreshold() {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, HUMIDITY_EXCEEDING_DATA));
        waitForProcessing();
        verify(logger).warn("ALARM: Humidity threshold exceeded! Sensor ID: {}, Value: {}", "h1", 55.0);
    }

    @Test
    public void testHumidityWithinThreshold() {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, HUMIDITY_NORMAL_DATA));
        waitForProcessing();
        verify(logger).info("Data is within safe thresholds. Sensor ID: {}, Value: {}", "h1", 45.0);
    }

    @Test
    public void testInvalidSensorData() {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, INVALID_DATA));
        waitForProcessing();
        verify(logger).error("Failed to parse sensor data: {}. Expected format: sensor_id=[sensorId];value=[value]", INVALID_DATA);
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        centralMonitoringService.stop(); 
        executorService.shutdown();

        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        if (kafkaProducer != null) kafkaProducer.close();
        if (kafkaConsumer != null) kafkaConsumer.close();
        if (kafkaContainer != null) kafkaContainer.stop();
    }

    private void waitForProcessing() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}