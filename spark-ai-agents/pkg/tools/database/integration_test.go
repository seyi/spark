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

//go:build integration
// +build integration

package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestPostgreSQLIntegration tests real PostgreSQL connection and queries.
func TestPostgreSQLIntegration(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Create config
	config := &DatabaseConfig{
		Type:             DatabaseTypePostgreSQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeReadOnly,
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    1000,
	}

	// Create connector
	connector, err := NewSparkSQLConnector(config)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Test connection
	if err := connector.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer connector.Disconnect(ctx)

	// Verify connected
	if !connector.IsConnected() {
		t.Error("Connector should be connected")
	}

	// Test simple query
	result, err := connector.ExecuteQuery(ctx, "SELECT 1 as num, 'test' as str")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Check data
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if row["num"] != int64(1) {
			t.Errorf("Expected num=1, got %v", row["num"])
		}
		if row["str"] != "test" {
			t.Errorf("Expected str='test', got %v", row["str"])
		}
	}

	// Check performance metrics
	if result.ExecutionTime <= 0 {
		t.Error("Execution time should be > 0")
	}

	t.Logf("Query executed in %.3f seconds", result.ExecutionTime)
}

// TestPostgreSQLTableOperations tests table creation and queries.
func TestPostgreSQLTableOperations(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(postgresContainer)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Create connector with FULL write mode
	config := &DatabaseConfig{
		Type:             DatabaseTypePostgreSQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeFull, // Allow writes
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    1000,
		BlockedKeywords:  []string{}, // Allow all operations
	}

	connector, err := NewSparkSQLConnector(config)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	if err := connector.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer connector.Disconnect(ctx)

	// Create table
	_, err = connector.ExecuteQuery(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			active BOOLEAN DEFAULT true
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = connector.ExecuteQuery(ctx, `
		INSERT INTO users (name, email, active) VALUES
		('Alice', 'alice@example.com', true),
		('Bob', 'bob@example.com', true),
		('Charlie', 'charlie@example.com', false)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query data
	result, err := connector.ExecuteQuery(ctx, "SELECT * FROM users WHERE active = true ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("Expected 2 active users, got %d", result.RowCount)
	}

	// Check first user
	if len(result.Rows) > 0 {
		alice := result.Rows[0]
		if alice["name"] != "Alice" {
			t.Errorf("Expected name='Alice', got %v", alice["name"])
		}
		if alice["email"] != "alice@example.com" {
			t.Errorf("Expected email='alice@example.com', got %v", alice["email"])
		}
	}

	// Test metadata discovery
	tables, err := connector.GetTables(ctx, "public")
	if err != nil {
		t.Fatalf("Failed to get tables: %v", err)
	}

	if len(tables) == 0 {
		t.Error("Expected at least one table")
	}

	t.Logf("Found tables: %v", tables)

	// Test table schema
	tableInfo, err := connector.GetTableInfo(ctx, "public", "users")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	if tableInfo.Table != "users" {
		t.Errorf("Expected table name 'users', got %s", tableInfo.Table)
	}

	if len(tableInfo.Columns) < 4 {
		t.Errorf("Expected at least 4 columns, got %d", len(tableInfo.Columns))
	}

	t.Logf("Table schema: %d columns", len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		t.Logf("  - %s: %s (nullable: %v)", col.Name, col.DataType, col.Nullable)
	}
}

// TestMySQLIntegration tests real MySQL connection and queries.
func TestMySQLIntegration(t *testing.T) {
	ctx := context.Background()

	// Start MySQL container
	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("testuser"),
		mysql.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start MySQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(mysqlContainer)

	// Get connection string
	connStr, err := mysqlContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Create config
	config := &DatabaseConfig{
		Type:             DatabaseTypeMySQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeReadOnly,
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    1000,
	}

	// Create connector
	connector, err := NewSparkSQLConnector(config)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Test connection
	if err := connector.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer connector.Disconnect(ctx)

	// Test simple query
	result, err := connector.ExecuteQuery(ctx, "SELECT 1 as num, 'test' as str")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount)
	}

	t.Logf("MySQL query executed in %.3f seconds", result.ExecutionTime)
}

// TestSecurityControls tests WriteMode and query validation.
func TestSecurityControls(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(postgresContainer)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Test BLOCKED mode
	t.Run("BlockedMode", func(t *testing.T) {
		config := &DatabaseConfig{
			Type:             DatabaseTypePostgreSQL,
			ConnectionString: connStr,
			WriteMode:        WriteModeBlocked,
			QueryTimeout:     30 * time.Second,
		}

		connector, _ := NewSparkSQLConnector(config)
		connector.Connect(ctx)
		defer connector.Disconnect(ctx)

		// SELECT should work
		_, err := connector.ExecuteQuery(ctx, "SELECT 1")
		if err != nil {
			t.Errorf("SELECT should be allowed in BLOCKED mode: %v", err)
		}

		// INSERT should fail
		_, err = connector.ExecuteQuery(ctx, "INSERT INTO test VALUES (1)")
		if err == nil {
			t.Error("INSERT should be blocked in BLOCKED mode")
		}
	})

	// Test READ_ONLY mode
	t.Run("ReadOnlyMode", func(t *testing.T) {
		config := &DatabaseConfig{
			Type:             DatabaseTypePostgreSQL,
			ConnectionString: connStr,
			WriteMode:        WriteModeReadOnly,
			QueryTimeout:     30 * time.Second,
		}

		connector, _ := NewSparkSQLConnector(config)
		connector.Connect(ctx)
		defer connector.Disconnect(ctx)

		// SELECT should work
		_, err := connector.ExecuteQuery(ctx, "SELECT 1")
		if err != nil {
			t.Errorf("SELECT should be allowed in READ_ONLY mode: %v", err)
		}

		// DROP should fail
		_, err = connector.ExecuteQuery(ctx, "DROP TABLE test")
		if err == nil {
			t.Error("DROP should be blocked in READ_ONLY mode")
		}
	})

	// Test blocked keywords
	t.Run("BlockedKeywords", func(t *testing.T) {
		config := &DatabaseConfig{
			Type:             DatabaseTypePostgreSQL,
			ConnectionString: connStr,
			WriteMode:        WriteModeFull,
			BlockedKeywords:  []string{"DROP", "TRUNCATE"},
			QueryTimeout:     30 * time.Second,
		}

		connector, _ := NewSparkSQLConnector(config)
		connector.Connect(ctx)
		defer connector.Disconnect(ctx)

		// DROP should be blocked
		_, err := connector.ExecuteQuery(ctx, "DROP TABLE test")
		if err == nil {
			t.Error("DROP should be blocked by blocked keywords")
		}

		// TRUNCATE should be blocked
		_, err = connector.ExecuteQuery(ctx, "TRUNCATE TABLE test")
		if err == nil {
			t.Error("TRUNCATE should be blocked by blocked keywords")
		}
	})
}

// TestPerformanceMetrics tests metric tracking.
func TestPerformanceMetrics(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(postgresContainer)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	config := &DatabaseConfig{
		Type:             DatabaseTypePostgreSQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeReadOnly,
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    1000,
	}

	connector, _ := NewSparkSQLConnector(config)
	connector.Connect(ctx)
	defer connector.Disconnect(ctx)

	// Execute multiple queries
	for i := 0; i < 5; i++ {
		_, err := connector.ExecuteQuery(ctx, fmt.Sprintf("SELECT %d", i))
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
	}

	// Check stats
	info := connector.GetConnectionInfo()

	queriesExecuted, ok := info["queries_executed"].(int64)
	if !ok || queriesExecuted != 5 {
		t.Errorf("Expected 5 queries executed, got %v", info["queries_executed"])
	}

	totalTime, ok := info["total_time_seconds"].(float64)
	if !ok || totalTime <= 0 {
		t.Errorf("Expected positive total time, got %v", info["total_time_seconds"])
	}

	t.Logf("Performance metrics:")
	t.Logf("  Queries executed: %v", info["queries_executed"])
	t.Logf("  Total time: %.3f seconds", info["total_time_seconds"])
	t.Logf("  Avg time per query: %.3f seconds", totalTime/float64(queriesExecuted))
}

// TestMaxResultRows tests result truncation.
func TestMaxResultRows(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(postgresContainer)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Config with small MaxResultRows
	config := &DatabaseConfig{
		Type:             DatabaseTypePostgreSQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeReadOnly,
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    5, // Only 5 rows max
	}

	connector, _ := NewSparkSQLConnector(config)
	connector.Connect(ctx)
	defer connector.Disconnect(ctx)

	// Query that would return 10 rows
	result, err := connector.ExecuteQuery(ctx, "SELECT generate_series(1, 10) as num")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should be truncated to 5
	if result.RowCount != 5 {
		t.Errorf("Expected 5 rows (truncated), got %d", result.RowCount)
	}

	// Should have warning
	if len(result.Warnings) == 0 {
		t.Error("Expected truncation warning")
	}

	t.Logf("Result truncated: %v", result.Warnings)
}

// TestDatabaseToolset tests the full toolset with real database.
func TestDatabaseToolset(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.TerminateContainer(postgresContainer)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	config := &DatabaseConfig{
		Type:             DatabaseTypePostgreSQL,
		ConnectionString: connStr,
		WriteMode:        WriteModeReadOnly,
		QueryTimeout:     30 * time.Second,
		MaxResultRows:    1000,
	}

	// Create toolset
	toolset, err := NewDatabaseToolset(config)
	if err != nil {
		t.Fatalf("Failed to create toolset: %v", err)
	}

	if err := toolset.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect toolset: %v", err)
	}
	defer toolset.Disconnect(ctx)

	tools := toolset.GetTools()
	if len(tools) != 7 {
		t.Errorf("Expected 7 tools, got %d", len(tools))
	}

	// Test list_databases tool
	t.Run("ListDatabases", func(t *testing.T) {
		listDbTool := tools[0]
		result, err := listDbTool.Handler(ctx, map[string]interface{}{})
		if err != nil {
			t.Fatalf("list_databases failed: %v", err)
		}

		resultMap := result.(map[string]interface{})
		databases := resultMap["databases"].([]string)
		if len(databases) == 0 {
			t.Error("Expected at least one database")
		}

		t.Logf("Found databases: %v", databases)
	})

	// Test execute_sql tool
	t.Run("ExecuteSQL", func(t *testing.T) {
		executeSQLTool := tools[4]
		result, err := executeSQLTool.Handler(ctx, map[string]interface{}{
			"query": "SELECT 1 as num, 'test' as str",
		})
		if err != nil {
			t.Fatalf("execute_sql failed: %v", err)
		}

		queryResult := result.(*QueryResult)
		if queryResult.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", queryResult.RowCount)
		}

		t.Logf("Query result: %d rows, %.3fs", queryResult.RowCount, queryResult.ExecutionTime)
	})
}
