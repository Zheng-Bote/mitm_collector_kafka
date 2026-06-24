# Changelog

All notable changes to the `mitm_collector_kafka` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-23

### Added
- Initial implementation of the Kafka Collector using `segmentio/kafka-go`.
- Support for SASL/PLAIN authentication and TLS (designed for Confluent Cloud compatibility).
- Dynamic fetching of broker host and credentials from `source_credentials` DB table.
- Envelope Encryption (AES-GCM) implemented for payload encryption at rest using the Master Key.
- Unix Socket IPC integration for reporting `started`, `processing`, and `audit` events to the central Scheduler.
- Configurable `idle_timeout_seconds` via CLI arguments to allow clean, automated termination when the data stream runs dry.
- Fallback logic to use the native Kafka message key if the JSON payload lacks a specific `business_key_column`.
