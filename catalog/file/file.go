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
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

const (
	// LocationKey is the property key for the base location of the catalog
	LocationKey = "file.location"
	// NamespacesKey is the property key for the location of the namespaces metadata file
	NamespacesKey = "file.namespaces"
	// TablesKey is the property key for the location of the tables metadata file
	TablesKey = "file.tables"
)

func init() {
	catalog.Register("file", catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (c catalog.Catalog, err error) {
		location, ok := p[LocationKey]
		if !ok {
			return nil, errors.New("must provide file.location property")
		}

		namespacesFile := p.Get(NamespacesKey, path.Join(location, "metadata", "namespaces.json"))
		tablesFile := p.Get(TablesKey, path.Join(location, "metadata", "tables.json"))

		return NewCatalog(name, location, namespacesFile, tablesFile, p)
	}))
}

var _ catalog.Catalog = (*Catalog)(nil)

// Catalog is a file-based implementation of the catalog.Catalog interface
// that stores Iceberg metadata files alongside the data files.
type Catalog struct {
	name          string
	location      string
	namespacesLoc string
	tablesLoc     string
	props         iceberg.Properties
	mu            sync.RWMutex
}

// NewCatalog creates a new file-based catalog with the given name and properties.
func NewCatalog(name, location, namespacesLoc, tablesLoc string, props iceberg.Properties) (*Catalog, error) {
	// Set default warehouse property if not provided
	if props.Get("warehouse", "") == "" {
		props["warehouse"] = location
	}

	return &Catalog{
		name:          name,
		location:      strings.TrimRight(location, "/"),
		namespacesLoc: namespacesLoc,
		tablesLoc:     tablesLoc,
		props:         props,
	}, nil
}

// CatalogType returns the type of the catalog.
func (c *Catalog) CatalogType() catalog.Type {
	return catalog.File
}

// namespaceMetadata represents the metadata for a namespace
type namespaceMetadata struct {
	Properties iceberg.Properties `json:"properties"`
}

// tableMetadata represents the metadata for a table
type tableMetadata struct {
	Name             string `json:"name"`
	Namespace        string `json:"namespace"`
	MetadataLocation string `json:"metadata_location"`
	Type             string `json:"type"` // "TABLE" or "VIEW"
}

// CreateTable creates a new iceberg table in the catalog using the provided identifier
// and schema. Options can be used to optionally provide location, partition spec, sort order,
// and custom properties.
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a staged table
	staged, err := internal.CreateStagedTable(ctx, c.props, c.loadNamespaceProperties, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	// Check if the namespace exists
	nsIdent := catalog.NamespaceFromIdent(identifier)
	tblIdent := catalog.TableNameFromIdent(identifier)
	nsExists, ns, _, err := c.checkNamespaceExists(ctx, nsIdent)
	if err != nil {
		return nil, err
	}

	if !nsExists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	// Check if the table already exists
	tableKey := getTableKey(ns, tblIdent)
	tableExists, _, _, tables, err := c.checkTableExists(ctx, identifier)
	if err != nil {
		return nil, err
	}

	if tableExists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, identifier)
	}

	// Write the table metadata
	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(iceio.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	// Add the table to the tables metadata
	tables[tableKey] = tableMetadata{
		Name:             tblIdent,
		Namespace:        ns,
		MetadataLocation: staged.MetadataLocation(),
		Type:             "TABLE",
	}

	// Save the tables metadata
	if err := c.saveTables(ctx, tables); err != nil {
		return nil, err
	}

	// Load and return the table
	return c.loadTable(ctx, identifier, staged.Properties())
}

// CommitTable commits the table metadata and updates to the catalog, returning the new metadata
func (c *Catalog) CommitTable(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get table information
	identifier := tbl.Identifier()
	ns := catalog.NamespaceFromIdent(identifier)
	tblName := catalog.TableNameFromIdent(identifier)
	nsStr := namespaceToString(ns)
	tableKey := getTableKey(nsStr, tblName)

	// Load the current table
	current, err := c.loadTable(ctx, identifier, nil)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	// Update and stage the table
	staged, err := internal.UpdateAndStageTable(ctx, current, identifier, reqs, updates, c)
	if err != nil {
		return nil, "", err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		// No changes, do nothing
		return current.Metadata(), current.MetadataLocation(), nil
	}

	// Write the metadata
	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, "", err
	}

	// Update the table metadata
	_, _, _, tables, err := c.checkTableExists(ctx, identifier)
	if err != nil {
		return nil, "", err
	}

	tables[tableKey] = tableMetadata{
		Name:             tblName,
		Namespace:        nsStr,
		MetadataLocation: staged.MetadataLocation(),
		Type:             "TABLE",
	}

	// Save the tables metadata
	if err := c.saveTables(ctx, tables); err != nil {
		return nil, "", err
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

// ListTables returns a list of table identifiers in the catalog, with the returned
// identifiers containing the information required to load the table via that catalog.
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Load the tables metadata
	tables, err := c.loadTables(ctx)
	if err != nil {
		return func(yield func(table.Identifier, error) bool) {
			yield(table.Identifier{}, err)
		}
	}

	// Check if the namespace exists
	nsPrefix := ""
	if len(namespace) > 0 {
		exists, ns, _, err := c.checkNamespaceExists(ctx, namespace)
		if err != nil {
			return func(yield func(table.Identifier, error) bool) {
				yield(table.Identifier{}, err)
			}
		}

		if !exists {
			return func(yield func(table.Identifier, error) bool) {
				yield(table.Identifier{}, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns))
			}
		}

		nsPrefix = ns + "."
	}

	// Filter tables by namespace
	var filteredTables []table.Identifier
	for key, tbl := range tables {
		if tbl.Type != "TABLE" {
			continue
		}

		if nsPrefix == "" || strings.HasPrefix(key, nsPrefix) {
			parts := strings.Split(key, ".")
			filteredTables = append(filteredTables, parts)
		}
	}

	// Return the filtered tables
	return func(yield func(table.Identifier, error) bool) {
		for _, ident := range filteredTables {
			if !yield(ident, nil) {
				return
			}
		}
	}
}

// LoadTable loads a table from the catalog and returns a Table with the metadata.
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.loadTable(ctx, identifier, props)
}

// DropTable tells the catalog to drop the table entirely.
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	exists, tableKey, _, tables, err := c.checkTableExists(ctx, identifier)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
	}

	// Remove the table from the tables metadata
	delete(tables, tableKey)

	// Save the tables metadata
	if err := c.saveTables(ctx, tables); err != nil {
		return err
	}

	return nil
}

// RenameTable tells the catalog to rename a given table by the identifiers
// provided, and then loads and returns the destination table
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the source table exists
	fromExists, fromTableKey, fromTableMeta, tables, err := c.checkTableExists(ctx, from)
	if err != nil {
		return nil, err
	}

	if !fromExists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, from)
	}

	// Check if the destination namespace exists
	toNs := catalog.NamespaceFromIdent(to)
	toTbl := catalog.TableNameFromIdent(to)
	toNsExists, toNsStr, _, err := c.checkNamespaceExists(ctx, toNs)
	if err != nil {
		return nil, err
	}

	if !toNsExists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toNsStr)
	}

	// Check if the destination table already exists
	toTableKey := getTableKey(toNsStr, toTbl)
	if _, exists := tables[toTableKey]; exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, to)
	}

	// Rename the table
	tables[toTableKey] = tableMetadata{
		Name:             toTbl,
		Namespace:        toNsStr,
		MetadataLocation: fromTableMeta.MetadataLocation,
		Type:             "TABLE",
	}
	delete(tables, fromTableKey)

	// Save the tables metadata
	if err := c.saveTables(ctx, tables); err != nil {
		return nil, err
	}

	// Load and return the renamed table
	return c.loadTable(ctx, to, nil)
}

// CheckTableExists returns if the table exists
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	exists, _, _, _, err := c.checkTableExists(ctx, identifier)
	return exists, err
}

// ListNamespaces returns the list of available namespaces, optionally filtering by a parent namespace
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Load the namespaces metadata
	namespaces, err := c.loadNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	// Filter namespaces by parent
	var filteredNamespaces []table.Identifier
	parentPrefix := ""
	if len(parent) > 0 {
		exists, parentNs, _, err := c.checkNamespaceExists(ctx, parent)
		if err != nil {
			return nil, err
		}

		if !exists {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, parentNs)
		}

		parentPrefix = parentNs + "."
	}

	for ns := range namespaces {
		if parentPrefix == "" || strings.HasPrefix(ns, parentPrefix) {
			parts := strings.Split(ns, ".")
			filteredNamespaces = append(filteredNamespaces, parts)
		}
	}

	return filteredNamespaces, nil
}

// CreateNamespace tells the catalog to create a new namespace with the given properties
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	exists, ns, namespaces, err := c.checkNamespaceExists(ctx, namespace)
	if err != nil {
		return err
	}

	// Check if the namespace already exists
	if exists {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, ns)
	}

	// Create the namespace
	if len(props) == 0 {
		props = iceberg.Properties{"exists": "true"}
	}

	namespaces[ns] = namespaceMetadata{
		Properties: props,
	}

	// Save the namespaces metadata
	return c.saveNamespaces(ctx, namespaces)
}

// DropNamespace tells the catalog to drop the namespace and all tables in that namespace
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	exists, ns, namespaces, err := c.checkNamespaceExists(ctx, namespace)
	if err != nil {
		return err
	}

	// Check if the namespace exists
	if !exists {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	// Check if there are any tables in the namespace
	tables, err := c.loadTables(ctx)
	if err != nil {
		return err
	}

	nsPrefix := ns + "."
	for key := range tables {
		if strings.HasPrefix(key, nsPrefix) {
			return fmt.Errorf("%w: namespace %s has tables", catalog.ErrNamespaceNotEmpty, ns)
		}
	}

	// Drop the namespace
	delete(namespaces, ns)

	// Save the namespaces metadata
	return c.saveNamespaces(ctx, namespaces)
}

// CheckNamespaceExists returns if the namespace exists
func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	exists, _, _, err := c.checkNamespaceExists(ctx, namespace)
	return exists, err
}

// LoadNamespaceProperties returns the current properties in the catalog for
// a given namespace
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.loadNamespaceProperties(ctx, namespace)
}

// UpdateNamespaceProperties allows removing, adding, and/or updating properties of a namespace
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	exists, ns, namespaces, err := c.checkNamespaceExists(ctx, namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	// Check if the namespace exists
	if !exists {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	// Get the namespace metadata
	nsMeta := namespaces[ns]

	// Update the namespace properties
	updatedProps, summary, err := getUpdatedPropsAndUpdateSummary(nsMeta.Properties, removals, updates)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	// Save the updated namespace properties
	nsMeta.Properties = updatedProps
	namespaces[ns] = nsMeta

	// Save the namespaces metadata
	if err := c.saveNamespaces(ctx, namespaces); err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	return summary, nil
}

// Common helper functions for namespace operations

// checkNamespaceExists checks if a namespace exists
func (c *Catalog) checkNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, string, map[string]namespaceMetadata, error) {
	if len(namespace) == 0 {
		return false, "", nil, fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}

	ns := namespaceToString(namespace)

	// Load the namespaces metadata
	namespaces, err := c.loadNamespaces(ctx)
	if err != nil {
		return false, ns, nil, err
	}

	// Check if the namespace exists
	_, exists := namespaces[ns]
	return exists, ns, namespaces, nil
}

// loadNamespaceProperties is an internal version of LoadNamespaceProperties
// that doesn't acquire a lock, to be used by methods that already hold a lock.
func (c *Catalog) loadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	exists, ns, namespaces, err := c.checkNamespaceExists(ctx, namespace)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	// Return the namespace properties
	return namespaces[ns].Properties, nil
}

// Common helper functions for table operations

// loadTable is an internal version of LoadTable that doesn't acquire a lock,
// to be used by methods that already hold a lock.
func (c *Catalog) loadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	exists, _, tableMeta, _, err := c.checkTableExists(ctx, identifier)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
	}

	// Load the table
	if props == nil {
		props = iceberg.Properties{}
	}

	tblProps := maps.Clone(c.props)
	maps.Copy(props, tblProps)

	return table.NewFromLocation(
		ctx,
		identifier,
		tableMeta.MetadataLocation,
		iceio.LoadFSFunc(tblProps, tableMeta.MetadataLocation),
		c,
	)
}

// loadTables loads the tables metadata from the file
func (c *Catalog) loadTables(ctx context.Context) (map[string]tableMetadata, error) {
	fs, err := iceio.LoadFS(ctx, c.props, c.tablesLoc)
	if err != nil {
		return nil, fmt.Errorf("failed to load filesystem for tables: %w", err)
	}

	// Check if the tables file exists
	file, err := fs.Open(c.tablesLoc)
	if err != nil {
		// If the file doesn't exist, return an empty map
		if os.IsNotExist(err) {
			return make(map[string]tableMetadata), nil
		}
		return nil, fmt.Errorf("failed to open tables file: %w", err)
	}
	defer file.Close()

	// Decode the tables file
	var tables map[string]tableMetadata
	if err := json.NewDecoder(file).Decode(&tables); err != nil {
		return nil, fmt.Errorf("failed to decode tables file: %w", err)
	}

	return tables, nil
}

// saveTables saves the tables metadata to the file
func (c *Catalog) saveTables(ctx context.Context, tables map[string]tableMetadata) error {
	wfs, err := c.getWriteFileSystem(ctx, c.tablesLoc)
	if err != nil {
		return fmt.Errorf("failed to get write filesystem for tables: %w", err)
	}

	// Create the directory if it doesn't exist
	dir := path.Dir(c.tablesLoc)
	if err := c.ensureDirectoryExists(ctx, dir); err != nil {
		return fmt.Errorf("failed to create directory for tables file: %w", err)
	}

	// Create the tables file
	file, err := wfs.Create(c.tablesLoc)
	if err != nil {
		return fmt.Errorf("failed to create tables file: %w", err)
	}
	defer file.Close()

	// Encode the tables file
	if err := json.NewEncoder(file).Encode(tables); err != nil {
		return fmt.Errorf("failed to encode tables file: %w", err)
	}

	return nil
}

// checkTableExists checks if a table exists
func (c *Catalog) checkTableExists(ctx context.Context, identifier table.Identifier) (bool, string, tableMetadata, map[string]tableMetadata, error) {
	ns := catalog.NamespaceFromIdent(identifier)
	tbl := catalog.TableNameFromIdent(identifier)
	nsStr := namespaceToString(ns)
	tableKey := getTableKey(nsStr, tbl)

	// Load the tables metadata
	tables, err := c.loadTables(ctx)
	if err != nil {
		return false, tableKey, tableMetadata{}, nil, err
	}

	// Check if the table exists
	tableMeta, exists := tables[tableKey]
	return exists && tableMeta.Type == "TABLE", tableKey, tableMeta, tables, nil
}

// Common helper functions for filesystem operations

// ensureDirectoryExists ensures that the directory exists
func (c *Catalog) ensureDirectoryExists(ctx context.Context, dir string) error {
	fs, err := iceio.LoadFS(ctx, c.props, dir)
	if err != nil {
		return fmt.Errorf("failed to load filesystem for directory: %w", err)
	}

	// Try to open the directory to check if it exists
	file, err := fs.Open(dir)
	if err == nil {
		// Directory exists
		file.Close()
		return nil
	}

	// If the error is not "not found", return it
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check if directory exists: %w", err)
	}

	// Directory doesn't exist, create it
	_, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return errors.New("filesystem IO does not support writing")
	}

	return nil
}

// getWriteFileSystem loads a filesystem and ensures it supports writing
func (c *Catalog) getWriteFileSystem(ctx context.Context, location string) (iceio.WriteFileIO, error) {
	fs, err := iceio.LoadFS(ctx, c.props, location)
	if err != nil {
		return nil, fmt.Errorf("failed to load filesystem for %s: %w", location, err)
	}

	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, errors.New("filesystem IO does not support writing")
	}

	return wfs, nil
}

// Common helper functions for namespace operations

// loadNamespaces loads the namespaces metadata from the file
func (c *Catalog) loadNamespaces(ctx context.Context) (map[string]namespaceMetadata, error) {
	fs, err := iceio.LoadFS(ctx, c.props, c.namespacesLoc)
	if err != nil {
		return nil, fmt.Errorf("failed to load filesystem for namespaces: %w", err)
	}

	// Check if the namespaces file exists
	file, err := fs.Open(c.namespacesLoc)
	if err != nil {
		// If the file doesn't exist, return an empty map
		if os.IsNotExist(err) {
			return make(map[string]namespaceMetadata), nil
		}
		return nil, fmt.Errorf("failed to open namespaces file: %w", err)
	}
	defer file.Close()

	// Decode the namespaces file
	var namespaces map[string]namespaceMetadata
	if err := json.NewDecoder(file).Decode(&namespaces); err != nil {
		return nil, fmt.Errorf("failed to decode namespaces file: %w", err)
	}

	return namespaces, nil
}

// saveNamespaces saves the namespaces metadata to the file
func (c *Catalog) saveNamespaces(ctx context.Context, namespaces map[string]namespaceMetadata) error {
	wfs, err := c.getWriteFileSystem(ctx, c.namespacesLoc)
	if err != nil {
		return fmt.Errorf("failed to get write filesystem for namespaces: %w", err)
	}

	// Create the directory if it doesn't exist
	dir := path.Dir(c.namespacesLoc)
	if err := c.ensureDirectoryExists(ctx, dir); err != nil {
		return fmt.Errorf("failed to create directory for namespaces file: %w", err)
	}

	// Create the namespaces file
	file, err := wfs.Create(c.namespacesLoc)
	if err != nil {
		return fmt.Errorf("failed to create namespaces file: %w", err)
	}
	defer file.Close()

	// Encode the namespaces file
	if err := json.NewEncoder(file).Encode(namespaces); err != nil {
		return fmt.Errorf("failed to encode namespaces file: %w", err)
	}

	return nil
}

// Other common helper functions

// getTableKey creates a table key from namespace string and table name
func getTableKey(ns, tableName string) string {
	return ns + "." + tableName
}

// getUpdatedPropsAndUpdateSummary updates properties and returns a summary of the changes
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, catalog.PropertiesUpdateSummary, error) {
	// Check for overlap between removals and updates
	var overlap []string
	for _, key := range removals {
		if _, ok := updates[key]; ok {
			overlap = append(overlap, key)
		}
	}
	if len(overlap) > 0 {
		return nil, catalog.PropertiesUpdateSummary{}, fmt.Errorf("conflict between removals and updates for keys: %v", overlap)
	}

	// Clone the current properties
	updatedProps := maps.Clone(currentProps)
	removed := make([]string, 0, len(removals))
	updated := make([]string, 0, len(updates))

	// Remove properties
	for _, key := range removals {
		if _, exists := updatedProps[key]; exists {
			delete(updatedProps, key)
			removed = append(removed, key)
		}
	}

	// Update properties
	for key, value := range updates {
		if updatedProps[key] != value {
			updated = append(updated, key)
			updatedProps[key] = value
		}
	}

	// Create summary
	missing := make([]string, 0)
	for _, key := range removals {
		found := false
		for _, r := range removed {
			if r == key {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, key)
		}
	}

	summary := catalog.PropertiesUpdateSummary{
		Removed: removed,
		Updated: updated,
		Missing: missing,
	}

	return updatedProps, summary, nil
}

// namespaceToString converts a namespace identifier to a string
func namespaceToString(namespace table.Identifier) string {
	return strings.Join(namespace, ".")
}
