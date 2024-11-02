package com.assignment.service;

import java.time.Duration;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

/**
 * Service responsible for monitoring temperature and humidity sensor data.
 * Reads data from a Kafka topic and logs warnings if thresholds are exceeded.
 */
public class CentralMonitoringService {

    private final Logger logger;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private volatile boolean running = true;

    // Pattern e.g. "sensor_id=t1;value=40"
    private static final Pattern SENSOR_DATA_PATTERN = Pattern.compile("sensor_id=(t\\d+|h\\d+);value=(.+)");
    private static final double TEMPERATURE_THRESHOLD = 35.0;
    private static final double HUMIDITY_THRESHOLD = 50.0;

    /**
     * Constructor for CentralMonitoringService.
     *
     * @param kafkaConsumer the Kafka consumer used to consume sensor data messages
     * @param logger the logger to use for logging events (kept as parameter to test assertions in unit and Intg tests
     */
    public CentralMonitoringService(KafkaConsumer<String, String> kafkaConsumer, Logger logger) {
        this.kafkaConsumer = kafkaConsumer;
        this.logger = logger;
        this.kafkaConsumer.subscribe(Collections.singletonList("sensor-data"));
    }

    /**
     * Starts the monitoring service. Polls the Kafka topic for sensor data, processes each record, and checks whether the data exceeds specified thresholds.
     * The method runs continuously until stop() is called.
     */
    public void start() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    processSensorData(record.value());
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    /**
     * Stops the monitoring service. This will break the polling loop and close the Kafka consumer to release resources.
     */
    public void stop() {
        running = false;
        logger.info("CentralMonitoringService is stopping.");
    }

    /**
     * This method is used to process the sensor data; also parses the data to extract the sensor ID and value, 
     * then checks the value against the threshold for either temperature or humidity, logging an alert if the threshold is exceeded.
     *
     * @param data the sensor data in the format "sensor_id=t1;value=40"
     */
    public void processSensorData(String data) {
        Matcher matcher = SENSOR_DATA_PATTERN.matcher(data);

        if (matcher.find()) {
            String sensorId = matcher.group(1);
            double value = Double.parseDouble(matcher.group(2));

            if (sensorId.startsWith("t") && value > TEMPERATURE_THRESHOLD) {
                logger.warn("ALARM: Temperature threshold exceeded! Sensor ID: {}, Value: {}", sensorId, value);
            } else if (sensorId.startsWith("h") && value > HUMIDITY_THRESHOLD) {
                logger.warn("ALARM: Humidity threshold exceeded! Sensor ID: {}, Value: {}", sensorId, value);
            } else {
                logger.info("Data is within safe thresholds. Sensor ID: {}, Value: {}", sensorId, value);
            }
        } else {
            logger.error("Failed to parse sensor data: {}. Expected format: sensor_id=[sensorId];value=[value]", data);
        }
    }
}