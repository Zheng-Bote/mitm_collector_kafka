# MitM Kafka Collector

The **Kafka Collector** is an autonomous component of the MitM (Man-in-the-Middle) Data Aggregator project. It reads records (e.g. employee master data) from an external Apache Kafka / Confluent Cloud stream, securely encrypts the payload using Envelope Encryption (AES-GCM), and stages the data as raw fragments inside the local MitM PostgreSQL database for further processing by the Transformation Layer.

## Features

- **Kafka Consumer:** Integrates with Kafka using `segmentio/kafka-go` via SASL/PLAIN and TLS.
- **Envelope Encryption:** Uses a Data Encryption Key (DEK) combined with the Master Key (KEK) to safely encrypt any PII data at rest before saving it to PostgreSQL.
- **Dynamic Configuration:** Fetches connection details (Broker, Key, Secret, Topic) securely at runtime from the `source_credentials` DB table.
- **IPC Scheduler Integration:** Reports runtime status, processing progress, and explicit audit events to the central scheduler daemon via Unix Domain Sockets.
- **Configurable Idle Timeout:** Automatically shuts down cleanly if no new messages are detected on the stream for a configurable duration (`idle_timeout_seconds`).

## Usage

The collector is designed to be executed via the central MitM Scheduler. The configuration is supplied via environment variables and JSON arguments.

### Arguments (JSON)
The following arguments can be passed via the `args` column in the `scheduled_programs` table:

```json
{
  "source_name": "KAFKA_EMPLOYEE",
  "topic": "bmw.hrmasterdata.Employee.v2",
  "business_key_column": "pernr",
  "idle_timeout_seconds": 60
}
```

- `source_name`: The identifier used to lookup the SASL connection credentials in the database.
- `topic`: The Kafka topic to subscribe to.
- `business_key_column`: The JSON attribute to use as a unique business key. If absent, the native Kafka message key is used.
- `idle_timeout_seconds`: Defaults to 30. How long to wait for new messages before successfully terminating the job.

### Environment Variables

| Variable | Description |
|---|---|
| `MITM_DB_HOST` | Target database host |
| `MITM_DB_PORT` | Target database port |
| `MITM_DB_USER` | Target database user |
| `MITM_DB_PASSWORD` | Target database password |
| `MITM_DB_NAME` | Target database name |
| `MASTER_KEY` | Key Encryption Key (KEK) for Envelope Encryption |
| `SCHEDULER_SOCKET_PATH` | Path to the Unix Domain Socket of the Scheduler |
| `RUN_ID` | Execution ID given by the Scheduler |

## Building

```bash
go build -o bin/mitm-collector-kafka ./main.go
```
