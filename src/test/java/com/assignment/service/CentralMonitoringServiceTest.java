package com.assignment.service;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class CentralMonitoringServiceTest {

    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;

    @Mock
    private Logger logger;

    private CentralMonitoringService centralMonitoringService;

    @BeforeEach
    void setUp() {
        
        centralMonitoringService = new CentralMonitoringService(kafkaConsumer, logger);
    }

    @Test
    void testTemperatureAboveThreshold_LogsWarning() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sensor-data", 0, 0, "key", "sensor_id=t1;value=40.0");

        centralMonitoringService.processSensorData(record.value());

        verify(logger).warn("ALARM: Temperature threshold exceeded! Sensor ID: {}, Value: {}", "t1", 40.0);
    }

    @Test
    void testHumidityAboveThreshold_LogsWarning() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sensor-data", 0, 0, "key", "sensor_id=h1;value=55.0");

        centralMonitoringService.processSensorData(record.value());

        verify(logger).warn("ALARM: Humidity threshold exceeded! Sensor ID: {}, Value: {}", "h1", 55.0);
    }

    @Test
    void testTemperatureBelowThreshold_NoWarning() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sensor-data", 0, 0, "key", "sensor_id=t1;value=30.0");

        centralMonitoringService.processSensorData(record.value());

        verify(logger, never()).warn(anyString(), anyString());
    }

    @Test
    void testHumidityBelowThreshold_NoWarning() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sensor-data", 0, 0, "key", "sensor_id=h1;value=45.0");

        centralMonitoringService.processSensorData(record.value());

        verify(logger, never()).warn(anyString(), anyString());
    }

    @Test
    void testInvalidData_LogsError() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("sensor-data", 0, 0, "key", "invalid_data_format");

        centralMonitoringService.processSensorData(record.value());

        verify(logger).error("Failed to parse sensor data: {}. Expected format: sensor_id=[sensorId];value=[value]", "invalid_data_format");
    }
}
