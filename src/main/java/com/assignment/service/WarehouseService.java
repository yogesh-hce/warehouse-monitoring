package com.assignment.service;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

/**
 * Service responsible for receiving UDP packets via a DatagramSocket and forwarding the received data to a Kafka topic.
 */
public class WarehouseService {

    private static final int MAX_RETRIES = 3;

    private final KafkaProducer<String, String> kafkaProducer;
    private final DatagramSocket datagramSocket;
    private final Logger logger;
    private volatile boolean running = true;

    /**
     * Constructor for WarehouseService.
     *
     * @param kafkaProducer the Kafka producer used to send messages to Kafka
     * @param datagramSocket the DatagramSocket for receiving UDP packets
     * @param logger the logger to use for logging events
     */
    public WarehouseService(KafkaProducer<String, String> kafkaProducer, DatagramSocket datagramSocket, Logger logger) {
        this.kafkaProducer = kafkaProducer;
        this.datagramSocket = datagramSocket;
        this.logger = logger;
    }

    /**
     * Starts the WarehouseService, which listens for UDP packets on the specified socket.
     * Received data is forwarded to a Kafka topic. 
     * This method runs continuously until the service is stopped or the maximum retry count is exceeded.
     */
    public void start() {
        listenOnPortWithRetry(MAX_RETRIES);
    }

    /**
     * Listens on the port associated with the DatagramSocket and retries in case of errors.
     * This method will stop attempting retries after the specified maximum retry count is exceeded.
     *
     * @param maxRetries the maximum number of retries if an error occurs
     */
    public void listenOnPortWithRetry(int maxRetries) {
        int port = datagramSocket.getLocalPort();
        int retryCount = 0;

        while (retryCount <= maxRetries && running) {
            try {
                byte[] receiveBuffer = new byte[1024];
                logger.info("WarehouseService started and listening on port {}", port);

                while (running) {
                    DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                    datagramSocket.receive(packet);
                    String receivedData = new String(packet.getData(), 0, packet.getLength());

                    logger.info("Received data: {}", receivedData);
                    sendToKafkaTopic(receivedData);
                }

            } catch (Exception e) {
                logger.error("Error while listening on port {}: {}", port, e.getMessage());
                retryCount++;

                if (retryCount > maxRetries) {
                    logger.error("Max retries exceeded. Stop re-try on port {}", port);
                    break;
                }

                logger.info("Retrying... Attempt {} of {}", retryCount, maxRetries);
                sleep(1000 * retryCount);
            }
        }

        // Ensure socket closure if the service stops or max retries are reached
        closeSocket();
    }

    /**
     * Sends the received data to the Kafka topic.
     *
     * @param data to be sent to Kafka
     */
    protected void sendToKafkaTopic(String data) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("sensor-data", data);
            kafkaProducer.send(record);
            logger.info("Sent data to Kafka: {}", data);
        } catch (Exception e) {
            logger.error("Failed to send data to Kafka: {}", e.getMessage());
        }
    }

    /**
     * Puts the current thread to sleep for a specified duration.
     *
     * @param delayInMillis the delay duration in milliseconds
     */
    protected void sleep(int delayInMillis) {
        try {
            Thread.sleep(delayInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during sleep: {}", e.getMessage());
        }
    }

    /**
     * Stops the WarehouseService by setting the running flag to false.
     */
    public void stop() {
        running = false;
        closeSocket();
        logger.info("WarehouseService is stopping.");
    }

    /**
     * Closes the DatagramSocket.
     */
    private void closeSocket() {
        if (!datagramSocket.isClosed()) {
            datagramSocket.close();
            logger.info("DatagramSocket on port {} closed.", datagramSocket.getLocalPort());
        }
    }
}