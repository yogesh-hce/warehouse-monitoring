package com.assignment.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class WarehouseServiceTest {

    @Mock
    private KafkaProducer<String, String> kafkaProducer;

    @Mock
    private DatagramSocket datagramSocket;

    @Mock
    private Logger logger;

    @InjectMocks
    private WarehouseService warehouseService;

    @BeforeEach
    public void setup() {
        warehouseService = new WarehouseService(kafkaProducer, datagramSocket, logger);
    }

    @Test
    public void testDataReceptionAndKafkaSend() {
     
        String testData = "sensor_id=t1;value=30.0";

        warehouseService.sendToKafkaTopic(testData);

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(recordCaptor.capture());

        ProducerRecord<String, String> capturedRecord = recordCaptor.getValue();
        assertEquals("sensor-data", capturedRecord.topic());
        assertEquals(testData, capturedRecord.value());

        verify(logger).info("Sent data to Kafka: {}", testData);
    }

    @Test
    public void testRetryMechanism() throws Exception {
        doThrow(new RuntimeException("Socket error")).when(datagramSocket).receive(any(DatagramPacket.class));

        WarehouseService spyService = spy(warehouseService);
        doNothing().when(spyService).sleep(anyInt());

        spyService.listenOnPortWithRetry(3);

        verify(spyService, times(3)).sleep(anyInt());
        verify(logger, times(3)).info(eq("Retrying... Attempt {} of {}"), anyInt(), eq(3));
    }


    @Test
    public void testMaxRetriesExceeded() throws Exception {
        doThrow(new RuntimeException("Socket error")).when(datagramSocket).receive(any(DatagramPacket.class));

        WarehouseService spyService = spy(warehouseService);
        doNothing().when(spyService).sleep(anyInt());

        spyService.listenOnPortWithRetry(3);

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);

        verify(logger).error(messageCaptor.capture(), valueCaptor.capture());
        
        assertEquals("Max retries exceeded. Stop re-try on port {}", messageCaptor.getValue());
        assertEquals(0, valueCaptor.getValue());
    }

    @Test
    public void testKafkaSendFailure() {
        doThrow(new RuntimeException("Kafka send failure")).when(kafkaProducer).send(any());

        String testData = "sensor_id=t1;value=25.0";
        warehouseService.sendToKafkaTopic(testData);

        verify(logger).error("Failed to send data to Kafka: {}", "Kafka send failure");
    }
}