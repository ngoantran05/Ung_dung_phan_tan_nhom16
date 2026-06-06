# Faust Distributed Stream Processing Project

## Overview

This project demonstrates a distributed stream processing system using Apache Kafka and Faust. The system simulates real-time sensor data generation, transmission, processing, and monitoring through a Kafka-based architecture.

## Technologies Used

- Python 3
- Apache Kafka
- Zookeeper
- Faust Streaming
- GitHub

## System Architecture

Producer
↓
Kafka Topic (sensor-data)
↓
Consumer
↓
Master

Additionally, Faust is used for real-time stream processing and statistics management.

## Project Structure

producer.py
consumer.py
master.py
app.py
README.md
Kafka/

## New Features

### Feature 1: Anomaly Detection

Detect abnormal sensor values when:

if value > 80

Example:

[WARNING] sensor_1 abnormal value: 95

### Feature 2: Average Statistics

Calculate average value for each sensor.

Average = Total Value / Number Of Messages

Example:

sensor_1: 50 messages | Average = 56.32

sensor_2: 45 messages | Average = 61.48

## Team Members

### Nguyễn Dương Ngọc Ánh

- Research Faust and Apache Kafka
- Write project report
- Design presentation slides
- Support code modification and testing

### Trần Thị Ngoãn

- Install Kafka and Zookeeper
- Develop Producer and Consumer
- Develop Master module
- Implement new features
- Manage GitHub repository

## Repository

https://github.com/ngoantran05/Ung_dung_phan_tan_nhom16

## Conclusion

This project successfully demonstrates a distributed stream processing system using Apache Kafka and Faust with real-time data processing, anomaly detection, and average statistics calculation.