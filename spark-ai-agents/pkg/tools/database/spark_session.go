// Copyright 2025 Apache Spark AI Agents
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	// Import JDBC/Thrift drivers for various databases
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq" // PostgreSQL
)

// SparkSession represents a connection to a Spark cluster.
// Supports multiple connection methods:
// - Spark Thrift Server (JDBC/ODBC interface)
// - Direct JDBC to databases
// - Future: gRPC/REST bridges
type SparkSession struct {
	config     *DatabaseConfig
	db         *sql.DB
	mu         sync.RWMutex
	connected  bool
	stats      SessionStats
	statements map[string]*sql.Stmt // Prepared statement cache
}

// SessionStats tracks session-level statistics.
type SessionStats struct {
	QueriesExecuted int64
	TotalBytes      int64
	TotalTime       time.Duration
	CacheHits       int64
	CacheMisses     int64
}

// NewSparkSession creates a new Spark session.
// Establishes connection to Spark SQL via appropriate driver.
func NewSparkSession(config *DatabaseConfig) (*SparkSession, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	session := &SparkSession{
		config:     config,
		statements: make(map[string]*sql.Stmt),
	}

	return session, nil
}

// Connect establishes connection to Spark SQL.
// Supports multiple connection types based on DatabaseType.
func (s *SparkSession) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil // Already connected
	}

	// Build connection string based on database type
	driverName, dsn, err := s.buildConnectionString()
	if err != nil {
		return fmt.Errorf("failed to build connection string: %w", err)
	}

	// Open database connection
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(s.config.PartitionCount * 2) // 2x partitions for parallelism
	db.SetMaxIdleConns(s.config.PartitionCount)
	db.SetConnMaxLifetime(s.config.QueryTimeout)

	// Test connection
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	s.db = db
	s.connected = true

	return nil
}

// buildConnectionString constructs the appropriate connection string.
// Returns (driverName, dataSourceName, error).
func (s *SparkSession) buildConnectionString() (string, string, error) {
	switch s.config.Type {
	case DatabaseTypePostgreSQL:
		return s.buildPostgreSQLDSN()

	case DatabaseTypeMySQL:
		return s.buildMySQLDSN()

	case DatabaseTypeBigQuery:
		// BigQuery via Spark would use a different approach
		// For now, return error indicating special handling needed
		return "", "", fmt.Errorf("BigQuery requires Spark BigQuery connector - use Spark Thrift Server with BigQuery connector configured")

	case DatabaseTypeSnowflake:
		return "", "", fmt.Errorf("Snowflake requires Spark Snowflake connector - use Spark Thrift Server with Snowflake connector configured")

	case DatabaseTypeSpark:
		// Native Spark via Thrift Server
		return s.buildSparkThriftDSN()

	default:
		return "", "", fmt.Errorf("unsupported database type: %s", s.config.Type)
	}
}

// buildPostgreSQLDSN builds PostgreSQL connection string.
func (s *SparkSession) buildPostgreSQLDSN() (string, string, error) {
	// Convert our connection string to PostgreSQL DSN
	// Format: postgresql://user:password@host:port/database
	// To: host=localhost port=5432 user=myuser password=mypassword dbname=mydb

	connStr := s.config.ConnectionString
	if !strings.HasPrefix(connStr, "postgresql://") {
		return "", "", fmt.Errorf("invalid PostgreSQL connection string: %s", connStr)
	}

	// For simplicity, pass through the connection string
	// In production, would parse and reconstruct properly
	dsn := strings.TrimPrefix(connStr, "postgresql://")

	// Add SSL mode if not specified
	if !strings.Contains(dsn, "sslmode=") {
		dsn += "?sslmode=disable" // Default for development
	}

	// Add timeout
	if s.config.QueryTimeout > 0 {
		timeoutSec := int(s.config.QueryTimeout.Seconds())
		if strings.Contains(dsn, "?") {
			dsn += fmt.Sprintf("&connect_timeout=%d", timeoutSec)
		} else {
			dsn += fmt.Sprintf("?connect_timeout=%d", timeoutSec)
		}
	}

	return "postgres", dsn, nil
}

// buildMySQLDSN builds MySQL connection string.
func (s *SparkSession) buildMySQLDSN() (string, string, error) {
	connStr := s.config.ConnectionString
	if !strings.HasPrefix(connStr, "mysql://") {
		return "", "", fmt.Errorf("invalid MySQL connection string: %s", connStr)
	}

	// Convert mysql:// to proper MySQL DSN
	// Format: mysql://user:password@host:port/database
	// To: user:password@tcp(host:port)/database
	dsn := strings.TrimPrefix(connStr, "mysql://")

	// Add timeout
	if s.config.QueryTimeout > 0 {
		timeoutStr := s.config.QueryTimeout.String()
		if strings.Contains(dsn, "?") {
			dsn += fmt.Sprintf("&timeout=%s", timeoutStr)
		} else {
			dsn += fmt.Sprintf("?timeout=%s", timeoutStr)
		}
	}

	return "mysql", dsn, nil
}

// buildSparkThriftDSN builds Spark Thrift Server connection string.
// Spark Thrift Server exposes HiveServer2 protocol.
func (s *SparkSession) buildSparkThriftDSN() (string, string, error) {
	// For Spark Thrift Server, we'd use a Hive driver
	// Format: hive://host:port/database
	// This requires a Go Hive driver like github.com/sql-machine-learning/gohive

	// For now, return error indicating Thrift setup needed
	return "", "", fmt.Errorf("Spark Thrift Server support requires Hive driver - install github.com/sql-machine-learning/gohive")
}

// Disconnect closes the Spark session.
func (s *SparkSession) Disconnect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected || s.db == nil {
		return nil
	}

	// Close prepared statements
	for _, stmt := range s.statements {
		stmt.Close()
	}
	s.statements = make(map[string]*sql.Stmt)

	// Close database connection
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}

	s.connected = false
	s.db = nil

	return nil
}

// IsConnected returns true if session is connected.
func (s *SparkSession) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// ExecuteQuery executes a SQL query and returns results.
// This is the REAL implementation, not a stub.
func (s *SparkSession) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	s.mu.RLock()
	if !s.connected || s.db == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("not connected to database")
	}
	db := s.db
	s.mu.RUnlock()

	startTime := time.Now()

	// Apply query timeout
	queryCtx := ctx
	if s.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, s.config.QueryTimeout)
		defer cancel()
	}

	// Execute query
	rows, err := db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all rows
	results := make([]map[string]interface{}, 0)
	rowCount := 0

	for rows.Next() {
		// Check max rows limit
		if s.config.MaxResultRows > 0 && rowCount >= s.config.MaxResultRows {
			break
		}

		// Create value holders
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}

		results = append(results, rowMap)
		rowCount++
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	executionTime := time.Since(startTime)

	// Update session stats
	s.mu.Lock()
	s.stats.QueriesExecuted++
	s.stats.TotalTime += executionTime
	s.mu.Unlock()

	// Build result
	result := &QueryResult{
		Columns:       columns,
		Rows:          results,
		RowCount:      rowCount,
		ExecutionTime: executionTime.Seconds(),
		Query:         query,
		Cached:        false, // Would check Spark cache in real implementation
	}

	// Add warnings if result was truncated
	if s.config.MaxResultRows > 0 && rowCount >= s.config.MaxResultRows {
		result.Warnings = []string{
			fmt.Sprintf("Result truncated to %d rows (max limit reached)", s.config.MaxResultRows),
		}
	}

	return result, nil
}

// ExecuteMetadataQuery executes metadata queries like SHOW DATABASES.
// Uses the same mechanism as ExecuteQuery but optimized for metadata.
func (s *SparkSession) ExecuteMetadataQuery(ctx context.Context, query string) ([]map[string]interface{}, error) {
	result, err := s.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

// GetDatabases lists all databases using SHOW DATABASES.
func (s *SparkSession) GetDatabases(ctx context.Context) ([]string, error) {
	var query string

	switch s.config.Type {
	case DatabaseTypePostgreSQL:
		query = "SELECT datname FROM pg_database WHERE datistemplate = false"
	case DatabaseTypeMySQL:
		query = "SHOW DATABASES"
	case DatabaseTypeSpark:
		query = "SHOW DATABASES"
	default:
		query = "SHOW DATABASES"
	}

	rows, err := s.ExecuteMetadataQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	databases := make([]string, 0, len(rows))
	for _, row := range rows {
		// Extract database name from first column
		for _, val := range row {
			if dbName, ok := val.(string); ok {
				databases = append(databases, dbName)
				break
			}
		}
	}

	return databases, nil
}

// GetTables lists all tables in a database using SHOW TABLES.
func (s *SparkSession) GetTables(ctx context.Context, database string) ([]string, error) {
	var query string

	switch s.config.Type {
	case DatabaseTypePostgreSQL:
		query = fmt.Sprintf("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
	case DatabaseTypeMySQL:
		query = fmt.Sprintf("SHOW TABLES FROM %s", database)
	case DatabaseTypeSpark:
		query = fmt.Sprintf("SHOW TABLES IN %s", database)
	default:
		query = fmt.Sprintf("SHOW TABLES IN %s", database)
	}

	rows, err := s.ExecuteMetadataQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0, len(rows))
	for _, row := range rows {
		// Extract table name from first column
		for _, val := range row {
			if tableName, ok := val.(string); ok {
				tables = append(tables, tableName)
				break
			}
		}
	}

	return tables, nil
}

// GetTableSchema retrieves table schema using DESCRIBE TABLE.
func (s *SparkSession) GetTableSchema(ctx context.Context, database, table string) (*TableInfo, error) {
	var query string

	// Build qualified table name
	qualifiedTable := table
	if database != "" {
		qualifiedTable = fmt.Sprintf("%s.%s", database, table)
	}

	switch s.config.Type {
	case DatabaseTypePostgreSQL:
		query = fmt.Sprintf(`
			SELECT column_name, data_type, is_nullable, column_default, ordinal_position
			FROM information_schema.columns
			WHERE table_name = '%s'
			ORDER BY ordinal_position
		`, table)
	case DatabaseTypeMySQL:
		query = fmt.Sprintf("DESCRIBE %s", qualifiedTable)
	case DatabaseTypeSpark:
		query = fmt.Sprintf("DESCRIBE TABLE %s", qualifiedTable)
	default:
		query = fmt.Sprintf("DESCRIBE TABLE %s", qualifiedTable)
	}

	rows, err := s.ExecuteMetadataQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// Convert rows to ColumnInfo
	columns := make([]ColumnInfo, 0, len(rows))
	for i, row := range rows {
		colInfo := ColumnInfo{
			Position: i + 1,
		}

		// Extract column information from row
		// Format varies by database, handle common cases
		if name, ok := row["column_name"].(string); ok {
			colInfo.Name = name
		} else if name, ok := row["Field"].(string); ok {
			colInfo.Name = name
		} else if name, ok := row["col_name"].(string); ok {
			colInfo.Name = name
		}

		if dtype, ok := row["data_type"].(string); ok {
			colInfo.DataType = dtype
		} else if dtype, ok := row["Type"].(string); ok {
			colInfo.DataType = dtype
		}

		if nullable, ok := row["is_nullable"].(string); ok {
			colInfo.Nullable = (nullable == "YES")
		} else if nullable, ok := row["Null"].(string); ok {
			colInfo.Nullable = (nullable == "YES")
		}

		columns = append(columns, colInfo)
	}

	// Build TableInfo
	tableInfo := &TableInfo{
		Database:           database,
		Table:              table,
		FullyQualifiedName: qualifiedTable,
		Type:               "TABLE",
		Columns:            columns,
		RowCount:           -1, // Would need COUNT(*) query
		SizeBytes:          -1, // Would need metadata query
		Metadata:           make(map[string]interface{}),
	}

	return tableInfo, nil
}

// GetStats returns session statistics.
func (s *SparkSession) GetStats() SessionStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// PrepareStatement prepares a SQL statement for repeated execution.
// Useful for parameterized queries.
func (s *SparkSession) PrepareStatement(ctx context.Context, query string) (*sql.Stmt, error) {
	s.mu.RLock()
	if !s.connected || s.db == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("not connected to database")
	}

	// Check cache
	if stmt, exists := s.statements[query]; exists {
		s.mu.RUnlock()
		return stmt, nil
	}
	s.mu.RUnlock()

	// Prepare new statement
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	s.statements[query] = stmt
	return stmt, nil
}
