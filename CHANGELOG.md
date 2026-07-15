# Changelog

All notable changes to the `mitm_collector_kafka` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.4.0] - 2026-07-15

### Added
- **IPC Logging Enhancements**: Added `Topic` and `SourceName` fields to `IPCClient` to consistently prefix all IPC messages with `<Topic>: <SourceName>: `. This aligns the logging format across all collectors.

## [v0.3.0] - 2026-07-07

### Added
- **SSL Support**: Added support for the `MITM_DB_SSLMODE` environment variable. The collector now respects this setting and applies it to the MitM PostgreSQL connection string.

## [v0.2.0] - 2026-06-30

### Changed
- **Config Restructuring**: Updated database connection logic to correctly parse the JSON configuration (`MITM_DB_CONFIG_JSON`) provided by the scheduler, accommodating the nested `"db"` object format.
- **Database Connection**: Prioritized the JSON configuration over direct environment variables. Direct variables (`MITM_DB_HOST`, etc.) now act solely as a fallback mechanism.
- **Audit Logging**: Implemented IPC audit logging (`ipc.SendAudit`) during the initialization phase to explicitly document whether the configuration was loaded via `JSON Config (MITM_DB_CONFIG_JSON)` or `Environment Variables`.

## [0.1.0] - 2026-06-23

### Added
- Initial implementation of the Kafka Collector using `segmentio/kafka-go`.
- Support for SASL/PLAIN authentication and TLS (designed for Confluent Cloud compatibility).
- Dynamic fetching of broker host and credentials from `source_credentials` DB table.
- Envelope Encryption (AES-GCM) implemented for payload encryption at rest using the Master Key.
- Unix Socket IPC integration for reporting `started`, `processing`, and `audit` events to the central Scheduler.
- Configurable `idle_timeout_seconds` via CLI arguments to allow clean, automated termination when the data stream runs dry.
- Fallback logic to use the native Kafka message key if the JSON payload lacks a specific `business_key_column`.
