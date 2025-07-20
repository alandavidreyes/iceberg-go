// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestCatalog creates a temporary directory and returns a file catalog for testing
func setupTestCatalog(t *testing.T) (*Catalog, string) {
	t.Helper()

	// Create a temporary directory for the catalog
	tempDir, err := os.MkdirTemp("", "iceberg-file-catalog-test-*")
	require.NoError(t, err, "Failed to create temp directory")

	// Create the metadata directory
	metadataDir := filepath.Join(tempDir, "metadata")
	err = os.MkdirAll(metadataDir, 0755)
	require.NoError(t, err, "Failed to create metadata directory")

	// Clean up the directory when the test is done
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	// Create the catalog
	props := iceberg.Properties{
		LocationKey: tempDir,
	}

	cat, err := NewCatalog("test-catalog", tempDir,
		filepath.Join(tempDir, "metadata", "namespaces.json"),
		filepath.Join(tempDir, "metadata", "tables.json"),
		props)
	require.NoError(t, err, "Failed to create catalog")

	return cat, tempDir
}

// createNamespace creates a test namespace in the catalog
func createNamespace(t *testing.T, ctx context.Context, cat *Catalog, ns string) {
	t.Helper()

	nsIdent := catalog.ToIdentifier(ns)
	err := cat.CreateNamespace(ctx, nsIdent, nil)
	require.NoError(t, err, "Failed to create namespace")
}

// createTable creates a test table in the catalog
func createTable(t *testing.T, ctx context.Context, cat *Catalog, ns, tblName string) *table.Table {
	t.Helper()

	// Create a simple schema
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Create the table
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier(ns, tblName), schema)
	require.NoError(t, err, "Failed to create table")

	return tbl
}

// TestCatalogCreation tests that a catalog can be created
func TestCatalogCreation(t *testing.T) {
	cat, _ := setupTestCatalog(t)

	assert.Equal(t, catalog.File, cat.CatalogType(), "Catalog type should be 'file'")
}

// TestCreateNamespace tests creating a namespace
func TestCreateNamespace(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")

	err := cat.CreateNamespace(ctx, nsIdent, nil)

	require.NoError(t, err, "Failed to create namespace")
}

// TestCheckNamespaceExists tests checking if a namespace exists
func TestCheckNamespaceExists(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")
	createNamespace(t, ctx, cat, "test_ns")

	exists, err := cat.CheckNamespaceExists(ctx, nsIdent)

	require.NoError(t, err, "Failed to check if namespace exists")
	assert.True(t, exists, "Namespace should exist")
}

// TestListNamespaces tests listing namespaces
func TestListNamespaces(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")
	createNamespace(t, ctx, cat, "test_ns")

	namespaces, err := cat.ListNamespaces(ctx, nil)

	require.NoError(t, err, "Failed to list namespaces")
	assert.Len(t, namespaces, 1, "Should have one namespace")
	assert.Equal(t, nsIdent, namespaces[0], "Namespace should match")
}

// TestLoadNamespaceProperties tests loading namespace properties
func TestLoadNamespaceProperties(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")
	createNamespace(t, ctx, cat, "test_ns")

	props, err := cat.LoadNamespaceProperties(ctx, nsIdent)

	require.NoError(t, err, "Failed to load namespace properties")
	assert.Equal(t, "true", props["exists"], "Namespace should have 'exists' property")
}

// TestUpdateNamespaceProperties tests updating namespace properties
func TestUpdateNamespaceProperties(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")
	createNamespace(t, ctx, cat, "test_ns")
	updates := iceberg.Properties{"key1": "value1"}

	summary, err := cat.UpdateNamespaceProperties(ctx, nsIdent, nil, updates)

	require.NoError(t, err, "Failed to update namespace properties")
	assert.Equal(t, []string{"key1"}, summary.Updated, "Should have updated 'key1'")

	// Verify the update
	props, err := cat.LoadNamespaceProperties(ctx, nsIdent)
	require.NoError(t, err, "Failed to load namespace properties")
	assert.Equal(t, "value1", props["key1"], "Property should be updated")
}

// TestCreateNestedNamespace tests creating a nested namespace
func TestCreateNestedNamespace(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	nestedNsIdent := catalog.ToIdentifier("test_ns.nested")

	err := cat.CreateNamespace(ctx, nestedNsIdent, nil)

	require.NoError(t, err, "Failed to create nested namespace")
}

// TestListNamespacesWithParent tests listing namespaces with a parent
func TestListNamespacesWithParent(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	createNamespace(t, ctx, cat, "test_ns.nested")

	namespaces, err := cat.ListNamespaces(ctx, catalog.ToIdentifier("test_ns"))

	require.NoError(t, err, "Failed to list namespaces with parent")
	assert.Len(t, namespaces, 1, "Should have one nested namespace")
	assert.Equal(t, catalog.ToIdentifier("test_ns.nested"), namespaces[0], "Nested namespace should match")
}

// TestDropNamespace tests dropping a namespace
func TestDropNamespace(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "empty_ns")

	err := cat.DropNamespace(ctx, catalog.ToIdentifier("empty_ns"))

	require.NoError(t, err, "Failed to drop empty namespace")

	// Verify the namespace was dropped
	exists, err := cat.CheckNamespaceExists(ctx, catalog.ToIdentifier("empty_ns"))
	require.NoError(t, err, "Failed to check if namespace exists")
	assert.False(t, exists, "Namespace should not exist after dropping")
}

// updateTableProperty is a helper function to update a table's properties
func updateTableProperty(t *testing.T, ctx context.Context, tbl *table.Table, key, value string) (*table.Table, error) {
	t.Helper()

	// Create a transaction
	tx := tbl.NewTransaction()

	// Update a property
	err := tx.SetProperties(iceberg.Properties{key: value})
	if err != nil {
		return nil, err
	}

	// Commit the transaction
	return tx.Commit(ctx)
}

// createTestSchema creates a simple schema for testing
func createTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

// TestCreateTable tests creating a table
func TestCreateTable(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	schema := createTestSchema()
	tblIdent := catalog.ToIdentifier("test_ns", "test_table")

	tbl, err := cat.CreateTable(ctx, tblIdent, schema)

	require.NoError(t, err, "Failed to create table")
	assert.NotNil(t, tbl, "Table should not be nil")
}

// TestCheckTableExists tests checking if a table exists
func TestCheckTableExists(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	tbl := createTable(t, ctx, cat, "test_ns", "test_table")
	tblIdent := tbl.Identifier()

	exists, err := cat.CheckTableExists(ctx, tblIdent)

	require.NoError(t, err, "Failed to check if table exists")
	assert.True(t, exists, "Table should exist")
}

// TestLoadTable tests loading a table
func TestLoadTable(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	tbl := createTable(t, ctx, cat, "test_ns", "test_table")
	tblIdent := tbl.Identifier()

	loadedTbl, err := cat.LoadTable(ctx, tblIdent, nil)

	require.NoError(t, err, "Failed to load table")
	assert.Equal(t, tbl.Identifier(), loadedTbl.Identifier(), "Table identifiers should match")
}

// TestListTables tests listing tables
func TestListTables(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	nsIdent := catalog.ToIdentifier("test_ns")
	createNamespace(t, ctx, cat, "test_ns")
	tbl := createTable(t, ctx, cat, "test_ns", "test_table")
	tblIdent := tbl.Identifier()

	tables := cat.ListTables(ctx, nsIdent)
	var tablesList []table.Identifier
	for ident, err := range tables {
		require.NoError(t, err, "Error in ListTables")
		tablesList = append(tablesList, ident)
	}

	assert.Len(t, tablesList, 1, "Should have one table")
	assert.Equal(t, tblIdent, tablesList[0], "Table identifier should match")
}

// TestRenameTable tests renaming a table
func TestRenameTable(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	createTable(t, ctx, cat, "test_ns", "test_table")
	fromIdent := catalog.ToIdentifier("test_ns", "test_table")
	toIdent := catalog.ToIdentifier("test_ns", "renamed_table")

	renamedTbl, err := cat.RenameTable(ctx, fromIdent, toIdent)

	require.NoError(t, err, "Failed to rename table")
	assert.Equal(t, toIdent, renamedTbl.Identifier(), "Renamed table identifier should match")
}

// TestDropTable tests dropping a table
func TestDropTable(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	tbl := createTable(t, ctx, cat, "test_ns", "test_table")
	tblIdent := tbl.Identifier()

	err := cat.DropTable(ctx, tblIdent)

	require.NoError(t, err, "Failed to drop table")

	// Verify the table was dropped
	exists, err := cat.CheckTableExists(ctx, tblIdent)
	require.NoError(t, err, "Failed to check if table exists")
	assert.False(t, exists, "Table should not exist after dropping")
}

// TestConcurrentWrites tests concurrent writes to different tables
func TestConcurrentWrites(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	tbl1 := createTable(t, ctx, cat, "test_ns", "table1")
	tbl2 := createTable(t, ctx, cat, "test_ns", "table2")

	var wg sync.WaitGroup
	wg.Add(2)

	// Start two goroutines to update the tables concurrently
	go func() {
		defer wg.Done()
		updatedTbl, err := updateTableProperty(t, ctx, tbl1, "client", "1")
		require.NoError(t, err, "Failed to update table 1")
		require.Equal(t, "1", updatedTbl.Properties()["client"], "Property should be updated")
	}()

	go func() {
		defer wg.Done()
		updatedTbl, err := updateTableProperty(t, ctx, tbl2, "client", "2")
		require.NoError(t, err, "Failed to update table 2")
		require.Equal(t, "2", updatedTbl.Properties()["client"], "Property should be updated")
	}()

	// Wait for both goroutines to complete
	wg.Wait()

	// Verify the updates
	loadedTbl1, err := cat.LoadTable(ctx, catalog.ToIdentifier("test_ns", "table1"), nil)
	require.NoError(t, err, "Failed to load table 1")
	assert.Equal(t, "1", loadedTbl1.Properties()["client"], "Table 1 property should be updated")

	loadedTbl2, err := cat.LoadTable(ctx, catalog.ToIdentifier("test_ns", "table2"), nil)
	require.NoError(t, err, "Failed to load table 2")
	assert.Equal(t, "2", loadedTbl2.Properties()["client"], "Table 2 property should be updated")
}

// TestConcurrentWritesToSameTable tests concurrent writes to the same table
func TestConcurrentWritesToSameTable(t *testing.T) {
	cat, _ := setupTestCatalog(t)
	ctx := context.Background()
	createNamespace(t, ctx, cat, "test_ns")
	createTable(t, ctx, cat, "test_ns", "shared_table")

	successCh := make(chan bool, 2)
	errorCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	// Start two goroutines to update the same table concurrently
	for i := 1; i <= 2; i++ {
		clientID := i
		go func() {
			defer wg.Done()

			// Load the table independently for each client
			clientTbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("test_ns", "shared_table"), nil)
			if err != nil {
				errorCh <- fmt.Errorf("client %d failed to load table: %w", clientID, err)
				successCh <- false
				return
			}

			// Update the table
			_, err = updateTableProperty(t, ctx, clientTbl, fmt.Sprintf("client%d", clientID), fmt.Sprintf("value%d", clientID))
			if err != nil {
				errorCh <- fmt.Errorf("client %d failed to update table: %w", clientID, err)
				successCh <- false
				return
			}

			successCh <- true
		}()
	}

	// Wait for both goroutines to complete
	wg.Wait()
	close(successCh)
	close(errorCh)

	// Count successful updates
	successCount := 0
	for success := range successCh {
		if success {
			successCount++
		}
	}

	// At least one client should succeed
	assert.GreaterOrEqual(t, successCount, 1, "At least one client should succeed")

	// Log additional information about the test results
	if successCount == 2 {
		t.Log("Both clients succeeded, updates were properly serialized")
	} else {
		var lastError error
		for err := range errorCh {
			if err != nil {
				lastError = err
			}
		}
		t.Logf("One client failed with error: %v", lastError)
	}

	// Verify the final state of the table
	loadedTbl, err := cat.LoadTable(ctx, catalog.ToIdentifier("test_ns", "shared_table"), nil)
	require.NoError(t, err, "Failed to load table")

	// The table should have at least one client property
	clientProps := 0
	if _, ok := loadedTbl.Properties()["client1"]; ok {
		clientProps++
	}
	if _, ok := loadedTbl.Properties()["client2"]; ok {
		clientProps++
	}

	assert.GreaterOrEqual(t, clientProps, 1, "Table should have at least one client property")
}
