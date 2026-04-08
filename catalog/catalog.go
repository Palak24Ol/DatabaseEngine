package catalog

import (
	"encoding/json"
	"fmt"
	"os"
)

type DataType int
const (TYPE_INT DataType = iota; TYPE_TEXT)

type ColumnConstraint int
const (CONSTRAINT_NONE ColumnConstraint = iota; CONSTRAINT_PRIMARY_KEY; CONSTRAINT_UNIQUE)

type ForeignKey struct {
	RefTable  string `json:"ref_table"`
	RefColumn string `json:"ref_column"`
}

type Column struct {
	Name       string
	DataType   DataType
	Constraint ColumnConstraint
	ForeignKey *ForeignKey
}

type TableSchema struct {
	TableName string
	Columns   []Column
}

func (s *TableSchema) GetColumnIndex(name string) (int, error) {
	for i, col := range s.Columns {
		if col.Name == name { return i, nil }
	}
	return -1, fmt.Errorf("column '%s' not found in table '%s'", name, s.TableName)
}

type IndexInfo struct {
	Name      string `json:"name"`
	TableName string `json:"table_name"`
	Column    string `json:"column"`
	Unique    bool   `json:"unique"`
}

type Catalog struct {
	tables    map[string]*TableSchema
	databases map[string]bool
	indexes   map[string]*IndexInfo
	filepath  string
}

type catalogDisk struct {
	Tables    []tableDisk `json:"tables"`
	Databases []string    `json:"databases"`
	Indexes   []IndexInfo `json:"indexes"`
}
type tableDisk struct {
	TableName string       `json:"table_name"`
	Columns   []columnDisk `json:"columns"`
}
type columnDisk struct {
	Name       string      `json:"name"`
	DataType   int         `json:"data_type"`
	Constraint int         `json:"constraint"`
	ForeignKey *ForeignKey `json:"foreign_key,omitempty"`
}

func NewCatalog(filepath string) *Catalog {
	return &Catalog{
		tables:    make(map[string]*TableSchema),
		databases: map[string]bool{"default": true},
		indexes:   make(map[string]*IndexInfo),
		filepath:  filepath,
	}
}

func (c *Catalog) Load() error {
	data, err := os.ReadFile(c.filepath)
	if os.IsNotExist(err) { return nil }
	if err != nil { return fmt.Errorf("failed to read catalog: %w", err) }
	var disk catalogDisk
	if err := json.Unmarshal(data, &disk); err != nil { return err }
	for _, t := range disk.Tables {
		var cols []Column
		for _, col := range t.Columns {
			cols = append(cols, Column{
				Name: col.Name, DataType: DataType(col.DataType),
				Constraint: ColumnConstraint(col.Constraint), ForeignKey: col.ForeignKey,
			})
		}
		c.tables[t.TableName] = &TableSchema{TableName: t.TableName, Columns: cols}
	}
	for _, db := range disk.Databases { c.databases[db] = true }
	for _, idx := range disk.Indexes { idxCopy := idx; c.indexes[idx.Name] = &idxCopy }
	fmt.Printf("📂 Loaded %d table(s), %d index(es) from catalog\n", len(c.tables), len(c.indexes))
	return nil
}

func (c *Catalog) Save() error {
	var disk catalogDisk
	for _, schema := range c.tables {
		var cols []columnDisk
		for _, col := range schema.Columns {
			cols = append(cols, columnDisk{
				Name: col.Name, DataType: int(col.DataType),
				Constraint: int(col.Constraint), ForeignKey: col.ForeignKey,
			})
		}
		disk.Tables = append(disk.Tables, tableDisk{TableName: schema.TableName, Columns: cols})
	}
	for db := range c.databases { disk.Databases = append(disk.Databases, db) }
	for _, idx := range c.indexes { disk.Indexes = append(disk.Indexes, *idx) }
	data, err := json.MarshalIndent(disk, "", "  ")
	if err != nil { return err }
	return os.WriteFile(c.filepath, data, 0644)
}

func (c *Catalog) CreateTable(schema *TableSchema) error {
	if _, exists := c.tables[schema.TableName]; exists {
		return fmt.Errorf("table '%s' already exists", schema.TableName)
	}
	c.tables[schema.TableName] = schema
	return c.Save()
}
func (c *Catalog) GetTable(name string) (*TableSchema, error) {
	schema, ok := c.tables[name]
	if !ok { return nil, fmt.Errorf("table '%s' does not exist", name) }
	return schema, nil
}
func (c *Catalog) HasTable(name string) bool { _, ok := c.tables[name]; return ok }
func (c *Catalog) GetAllTables() []*TableSchema {
	var tables []*TableSchema
	for _, t := range c.tables { tables = append(tables, t) }
	return tables
}
func (c *Catalog) CreateDatabase(name string) error {
	if c.databases[name] { return fmt.Errorf("database '%s' already exists", name) }
	c.databases[name] = true
	return c.Save()
}
func (c *Catalog) GetAllDatabases() []string {
	var dbs []string
	for db := range c.databases { dbs = append(dbs, db) }
	return dbs
}
func (c *Catalog) AddIndex(info IndexInfo) { c.indexes[info.Name] = &info; c.Save() }
func (c *Catalog) DropIndex(name string) { delete(c.indexes, name); c.Save() }
func (c *Catalog) GetIndex(name string) *IndexInfo { return c.indexes[name] }
func (c *Catalog) GetAllIndexes() []*IndexInfo {
	var result []*IndexInfo
	for _, idx := range c.indexes { result = append(result, idx) }
	return result
}
