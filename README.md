# Warehouse Monitoring System

This project is a **Warehouse Monitoring System** that uses **Kafka** and **UDP** to monitor temperature and humidity levels in a warehouse environment. The application consists of two main services:

- **WarehouseService**: Receives UDP packets with sensor data and forwards them to a Kafka topic.
- **CentralMonitoringService**: Consumes data from the Kafka topic, processes it, and logs alerts if thresholds are exceeded.

## Overview

The system is designed to monitor temperature and humidity levels through UDP packets sent by sensors. These packets are collected by the `WarehouseService`, which sends the data to a Kafka topic.
`CentralMonitoringService` then consumes this data, checks it against predefined thresholds, and logs alerts when the thresholds are exceeded.

## Technologies

- **Java** 17
- **Apache Kafka** (Producer and Consumer)
- **UDP** (User Datagram Protocol)
- **SLF4J + Logback** (Logging)
- **JUnit + Mockito** (Unit and Integration Testing)
- **Testcontainers** (For Integration tests)

## Unit-Tests
This project includes comprehensive unit tests and integration tests to ensure the core logic functions as expected. To run the tests: mvn test

## Integration tests (which use Testcontainers to create a temporary Kafka instance) 
It can be executed with: mvn clean verify -PintegrationTests. You should have Docker Desktop running in windows/wsl to run the IT tests.

## Note on Functional Testing in Different Environments
While I have created a strong suite of unit and integration tests, deploying this application in a functional testing environment requires additional setup, such as configuring a Kafka broker and other necessary infrastructure. For the purposes of this project, the primary focus was on implementing the main logic and ensuring correctness through unit and integration tests rather than full deployment setup.