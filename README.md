## Real-Time Credit Card Fraud Detection Pipeline
This project implements an end-to-end real-time data pipeline for detecting credit card fraud using Apache Kafka, Apache Spark, and Hadoop. It processes both historical and streaming transaction data, applies rule-based validation (UCL, credit score, ZIP distance), and updates customer records in real-time. The system simulates real-world POS streams, ingests AWS RDS data, and leverages Spark Streaming for scalable processing and fraud detection.

Key Features:

Real-time fraud detection using Spark and Kafka

Lookup table with UCL, credit score, and transaction history

Rule-based validation for secure, low-latency decision-making

Integration with NoSQL and Hadoop for large-scale data handling
