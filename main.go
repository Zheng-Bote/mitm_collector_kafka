/**
 * SPDX-FileComment: Kafka Collector
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.go
 * @brief Autonomous collector retrieving employee data from a Kafka topic, encrypting it, and saving it to RAW tables.
 * @version 1.0.0
 * @date 2026-06-23
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @license Apache-2.0
 */

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var (
	appName        = "Kafka Collector"
	appDescription = "Retrieves employee data from a Kafka topic"
	version        = "1.0.0"
)

// TargetDBConfig defines parameters for the MitM target database passed via JSON CLI argument
type TargetDBConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
	Database   string `json:"database"`
	DSN        string `json:"dsn"`
	SourceName string `json:"source_name"` // Defaults to "KAFKA_EMPLOYEE"
}

// SourceDBConfig defines decrypted credentials for the source Kafka loaded from source_credentials
type SourceDBConfig struct {
	Host     string `json:"host"` // Broker URL
	Port     int    `json:"port"`
	User     string `json:"user"`     // SASL Key
	Password string `json:"password"` // SASL Secret
	Database string `json:"database"` // Topic
	DSN      string `json:"dsn"`
}

// CollectorArgs defines optional runtime arguments passed by the scheduler as JSON
type CollectorArgs struct {
	SourceName        string `json:"source_name"`
	Table             string `json:"table"` // Used to derive topicName if Topic is empty
	CursorColumn      string `json:"cursor_column"` // Unused in Kafka but kept for compatibility
	Topic             string `json:"topic"`
	BusinessKeyColumn string `json:"business_key_column"`
	IdleTimeoutSecs   int    `json:"idle_timeout_seconds"`
}

// StatusEvent is sent to the scheduler Unix socket
type StatusEvent struct {
	RunID     int    `json:"run_id"`
	Type      string `json:"type"` // "status" (default) or "audit"
	Component string `json:"component"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Progress  int    `json:"progress"`
}

// IPCClient is used to send events to the scheduler
type IPCClient struct {
	SocketPath string
	RunID      int
	Component  string
}

func (c *IPCClient) SendEvent(status, message string, progress int) {
	if c == nil || c.SocketPath == "" {
		return
	}
	conn, err := net.Dial("unix", c.SocketPath)
	if err != nil {
		log.Printf("[IPC ERROR] Failed to connect to scheduler socket: %v", err)
		return
	}
	defer conn.Close()

	event := StatusEvent{
		RunID:    c.RunID,
		Type:     "status",
		Status:   status,
		Message:  message,
		Progress: progress,
	}
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func (c *IPCClient) SendAudit(message string) {
	if c == nil || c.SocketPath == "" {
		return
	}
	conn, err := net.Dial("unix", c.SocketPath)
	if err != nil {
		log.Printf("[IPC ERROR] Failed to connect to scheduler socket: %v", err)
		return
	}
	defer conn.Close()

	event := StatusEvent{
		RunID:     c.RunID,
		Type:      "audit",
		Component: c.Component,
		Message:   message,
	}
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func main() {
	// 2. Load IPC Environment
	var ipc *IPCClient
	runIDStr := os.Getenv("RUN_ID")
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	if runIDStr != "" && socketPath != "" {
		runID, err := strconv.Atoi(runIDStr)
		if err == nil {
			ipc = &IPCClient{
				SocketPath: socketPath,
				RunID:      runID,
				Component:  "mitm_collector_kafka",
			}
		}
	}

	ipc.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, version), 0)
	ipc.SendAudit(fmt.Sprintf("%s (%s) started", appName, version))

	// 3. Parse Target DB configuration
	var targetCfg TargetDBConfig
	configSource := "Environment Variables"
	jsonConfig := os.Getenv("MITM_DB_CONFIG_JSON")
	
	if jsonConfig != "" {
		var fullCfg struct {
			DB struct {
				Host     string `json:"host"`
				Port     int    `json:"port"`
				User     string `json:"user"`
				Password string `json:"password"`
				Database string `json:"database"`
			} `json:"db"`
		}
		if err := json.Unmarshal([]byte(jsonConfig), &fullCfg); err != nil {
			if ipc != nil {
				ipc.SendEvent("failed", fmt.Sprintf("Failed to parse MitM database JSON config: %v", err), 0)
			}
			log.Fatalf("Failed to parse MitM JSON configuration: %v", err)
		}
		targetCfg.Host = fullCfg.DB.Host
		targetCfg.Port = fullCfg.DB.Port
		targetCfg.User = fullCfg.DB.User
		targetCfg.Password = fullCfg.DB.Password
		targetCfg.Database = fullCfg.DB.Database
		configSource = "JSON Config (MITM_DB_CONFIG_JSON)"
	} else {
		targetCfg.Host = os.Getenv("MITM_DB_HOST")
		if portStr := os.Getenv("MITM_DB_PORT"); portStr != "" {
			targetCfg.Port, _ = strconv.Atoi(portStr)
		}
		targetCfg.User = os.Getenv("MITM_DB_USER")
		targetCfg.Password = os.Getenv("MITM_DB_PASSWORD")
		targetCfg.Database = os.Getenv("MITM_DB_NAME")
	}

	if targetCfg.Host == "" {
		if ipc != nil {
			ipc.SendEvent("failed", "MitM database configuration missing in ENV", 0)
		}
		log.Fatal("MitM database credentials not found in environment (MITM_DB_HOST or MITM_DB_CONFIG_JSON)")
	}

	if ipc != nil {
		ipc.SendAudit(fmt.Sprintf("Loaded database configuration from %s", configSource))
	}

	// 3b. Parse optional collector arguments from scheduler (now in os.Args[1])
	topicName := ""
	businessKeyCol := ""
	idleTimeout := 30 * time.Second

	if len(os.Args) >= 2 {
		var colArgs CollectorArgs
		if err := json.Unmarshal([]byte(os.Args[1]), &colArgs); err == nil {
			if colArgs.SourceName != "" {
				targetCfg.SourceName = colArgs.SourceName
			}
			if colArgs.Topic != "" {
				topicName = colArgs.Topic
			}
			if colArgs.BusinessKeyColumn != "" {
				businessKeyCol = colArgs.BusinessKeyColumn
			}
			if colArgs.IdleTimeoutSecs > 0 {
				idleTimeout = time.Duration(colArgs.IdleTimeoutSecs) * time.Second
			}
		} else {
			log.Printf("Warning: Failed to parse collector arguments from os.Args[1]: %v", err)
		}
	}

	if targetCfg.SourceName == "" {
		targetCfg.SourceName = "KAFKA_EMPLOYEE"
	}

	var mitmDSN string
	if targetCfg.DSN != "" {
		mitmDSN = targetCfg.DSN
	} else {
		sslMode := "disable"
		if os.Getenv("MITM_DB_SSLMODE") == "true" {
			sslMode = "require"
		}
		mitmDSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			targetCfg.User, targetCfg.Password, targetCfg.Host, targetCfg.Port, targetCfg.Database, sslMode)
	}

	ctx := context.Background()

	// 4. Connect to MitM target database
	mitmPool, err := pgxpool.New(ctx, mitmDSN)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to connect to MitM database: %v", err), 0)
		log.Fatalf("Failed to connect to MitM database: %v", err)
	}
	defer mitmPool.Close()

	ipc.SendEvent("processing", "Connected to MitM database", 20)

	// 5. Load KEK from environment
	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		ipc.SendEvent("failed", "Missing MASTER_KEY environment variable", 0)
		log.Fatal("Missing MASTER_KEY environment variable")
	}

	var kek []byte
	if decoded, err := base64.StdEncoding.DecodeString(masterKey); err == nil {
		kek = decoded
	} else {
		kek = []byte(masterKey)
	}

	// Adjust KEK to 32 bytes if necessary
	if len(kek) != 32 {
		adjusted := make([]byte, 32)
		copy(adjusted, kek)
		kek = adjusted
	}

	// 6. Query encrypted source credentials
	var configPayload []byte
	var credentialsNonce []byte
	var dekID string

	err = mitmPool.QueryRow(ctx, `
		SELECT config_payload, nonce, dek_id 
		FROM source_credentials 
		WHERE source_name = $1 AND is_active = true 
		LIMIT 1
	`, targetCfg.SourceName).Scan(&configPayload, &credentialsNonce, &dekID)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to load source credentials for '%s': %v", targetCfg.SourceName, err), 0)
		log.Fatalf("Failed to load source credentials: %v", err)
	}

	// 7. Query wrapped DEK
	var wrappedKey []byte
	err = mitmPool.QueryRow(ctx, `
		SELECT wrapped_key 
		FROM storage_keys 
		WHERE id = $1 AND is_active = true 
		LIMIT 1
	`, dekID).Scan(&wrappedKey)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to load wrapped DEK (ID: %s): %v", dekID, err), 0)
		log.Fatalf("Failed to load wrapped DEK: %v", err)
	}

	// 8. Decrypt wrapped DEK using KEK
	if len(wrappedKey) < 12 {
		ipc.SendEvent("failed", "Wrapped DEK is too short (must be at least 12 bytes nonce + cipher)", 0)
		log.Fatal("Wrapped DEK in database is invalid")
	}
	dekNonce := wrappedKey[:12]
	wrappedCipher := wrappedKey[12:]

	kekBlock, err := aes.NewCipher(kek)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize AES cipher with KEK: %v", err), 0)
		log.Fatalf("Failed to initialize AES cipher: %v", err)
	}
	kekGCM, err := cipher.NewGCM(kekBlock)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize GCM with KEK: %v", err), 0)
		log.Fatalf("Failed to initialize GCM: %v", err)
	}
	dek, err := kekGCM.Open(nil, dekNonce, wrappedCipher, nil)
	if err != nil {
		ipc.SendEvent("failed", "Failed to decrypt wrapped DEK (KEK mismatch or corrupted key data)", 0)
		log.Fatalf("Failed to decrypt DEK: %v", err)
	}

	ipc.SendAudit("Decrypted storage DEK using KEK successfully")

	// 9. Decrypt source connection credentials payload using DEK
	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize AES cipher with DEK: %v", err), 0)
		log.Fatalf("Failed to initialize DEK AES cipher: %v", err)
	}
	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize GCM with DEK: %v", err), 0)
		log.Fatalf("Failed to initialize DEK GCM: %v", err)
	}
	decryptedConfigBytes, err := dekGCM.Open(nil, credentialsNonce, configPayload, nil)
	if err != nil {
		ipc.SendEvent("failed", "Failed to decrypt source config payload using DEK", 0)
		log.Fatalf("Failed to decrypt source config: %v", err)
	}

	ipc.SendAudit("Decrypted source connection credentials payload successfully")

	// 10. Parse source Kafka configuration
	var sourceCfg SourceDBConfig
	if err := json.Unmarshal(decryptedConfigBytes, &sourceCfg); err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to parse decrypted source database configuration: %v", err), 0)
		log.Fatalf("Failed to parse decrypted source config: %v", err)
	}

	if topicName == "" {
		if sourceCfg.Database != "" {
			topicName = sourceCfg.Database
		} else {
			topicName = "bmw.hrmasterdata.Employee.v2"
		}
	}

	// 11. Connect to Kafka
	saslMechanism := plain.Mechanism{
		Username: sourceCfg.User,
		Password: sourceCfg.Password,
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           &tls.Config{},
		SASLMechanism: saslMechanism,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{sourceCfg.Host},
		GroupID:        "mitm.aggregator.collector",
		Topic:          topicName,
		Dialer:         dialer,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	ipc.SendEvent("processing", "Connected to source Kafka", 50)
	ipc.SendAudit(fmt.Sprintf("Connected to source Kafka topic %q successfully", topicName))

	// 12. Iterate and ingest messages dynamically
	recordsIngested := 0
	ipc.SendEvent("processing", "Reading Kafka messages", 70)

	for {
		// Wait for up to 30 seconds for a new message
		readCtx, readCancel := context.WithTimeout(ctx, idleTimeout)
		msg, err := reader.ReadMessage(readCtx)
		readCancel()

		if err != nil {
			if readCtx.Err() == context.DeadlineExceeded {
				log.Printf("No new messages for %v, breaking loop.", idleTimeout)
				break
			}
			ipc.SendEvent("failed", fmt.Sprintf("Failed to read from Kafka: %v", err), 0)
			log.Fatalf("Failed to read message from Kafka: %v", err)
		}

		// Parse JSON value to find business key
		data := make(map[string]interface{})
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			log.Printf("Warning: message value is not valid JSON: %v", err)
		}

		var businessKey string
		if businessKeyCol != "" {
			if bkVal, ok := data[businessKeyCol]; ok && bkVal != nil {
				businessKey = fmt.Sprintf("%v", bkVal)
			}
		}
		if businessKey == "" {
			if len(msg.Key) > 0 {
				businessKey = string(msg.Key)
			} else {
				businessKey = "UNKNOWN"
			}
		}

		// Generate random 12-byte nonce
		nonce := make([]byte, 12)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			log.Printf("Failed to generate random nonce: %v", err)
			continue
		}

		// Encrypt payload via AES-GCM using storage DEK
		encryptedPayload := dekGCM.Seal(nil, nonce, msg.Value, nil)

		// Generate deterministic Correlation ID
		namespaceMitM := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
		correlationID := uuid.NewSHA1(namespaceMitM, []byte(businessKey))

		// Insert into raw_ingestion in target database
		_, err = mitmPool.Exec(ctx, `
			INSERT INTO raw_ingestion (topic, source_system, correlation_id, payload, nonce, dek_id, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending')
		`, topicName, targetCfg.SourceName, correlationID, encryptedPayload, nonce, dekID)
		if err != nil {
			log.Printf("Failed to insert raw fragment: %v", err)
			continue
		}

		recordsIngested++
	}

	// 13. Finish execution
	ipc.SendAudit(fmt.Sprintf("%s (%s) finished", appName, version))
	ipc.SendEvent("finished", fmt.Sprintf("Successfully processed and ingested %d Kafka messages into RAW table", recordsIngested), 100)
	log.Printf("Collector finished. Ingested %d messages.", recordsIngested)
}
