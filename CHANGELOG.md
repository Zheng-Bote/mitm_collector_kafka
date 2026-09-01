# Changelog

All notable changes to the `mitm_collector_kafka` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.8.3] - 2026-09-01

### Fixed

- **IPC SSLMode DSN Fix**: Fixed an issue where the constructed database connection string (DSN) would incorrectly overwrite `MITM_DB_SSLMODE=require` with `disable`, which caused `FATAL: no encryption` errors in AWS RDS.

## [v0.8.2] - 2026-09-01

### Fixed

- **IPC SSLMode Type Fix**: Changed `SSLMode` field in JSON parsing struct from `string` to `bool` to correctly unmarshal boolean values (`true`/`false`) sent by the scheduler.

## [v0.8.1] - 2026-09-01

### Fixed

- **IPC SSLMode Fix**: Fixed an issue where `SSLMode` was not correctly parsed from the scheduler's JSON configuration and improved the `MITM_DB_SSLMODE` fallback logic to support proper PostgreSQL sslmode strings (e.g., `require`, `verify-full`).

## [v0.8.0] - 2026-08-31

### Added

- **IPC Socket as Credential Broker**: The collector now fetches database credentials and the master key at runtime from the Scheduler via a Unix Domain Socket request (`get_credentials` with `RUN_ID` and `SCHEDULER_SOCKET_PATH`), instead of holding them locally.

### Changed

- **Kafka Offset Safety**: Implemented transactional PostgreSQL boundaries for batch ingestion. Batch inserts and Kafka message commits now execute within a `pgx.Tx` transaction, and `CommitMessages()` is strictly skipped if any PostgreSQL partial failure occurs.

## [v0.7.0] - 2026-08-29

### Changed

- **Components Logging**: Refactored component version logging mechanism across all layers (Collectors, Transformation, Delivery, Scheduler) to consistently output a clean `Major.Minor.Patch` version format.

## [v0.6.0] - 2026-08-29

### Changed/Added

- Configured `pgxpool` connection limits (`MaxConns=20`, `MaxConnIdleTime=5m`, `MaxConnLifetime=1h`).
- Implemented graceful shutdown with context cancellation on `SIGINT`/`SIGTERM`.
- Optimized performance with batched operations.

## [v0.5.0] - 2026-07-29

### Changed

- **Correlation ID Fallback**: Changed the fallback for `correlation_id` from a hardcoded `"UNKNOWN"` string to a dynamically generated UUID (`uuid.New().String()`). This critical fix prevents rows with missing or NULL business keys from being falsely aggregated into a single record by the Transformation Engine.

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
