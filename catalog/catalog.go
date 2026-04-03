package catalog

import (
	"encoding/json"
	"fmt"
	"os"
)

type DataType int

const (
	TYPE_INT  DataType = iota
	TYPE_TEXT
)

type ColumnConstraint int

const (
	CONSTRAINT_NONE        ColumnConstraint = iota
	CONSTRAINT_PRIMARY_KEY
	CONSTRAINT_UNIQUE
)

type Column struct {
	Name       string
	DataType   DataType
	Constraint ColumnConstraint
}

type TableSchema struct {
	TableName string
	Columns   []Column
}

func (s *TableSchema) GetColumnIndex(name string) (int, error) {
	for i, col := range s.Columns {
		if col.Name == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column '%s' not found in table '%s'", name, s.TableName)
}

// Catalog stores schemas + supports multiple databases
type Catalog struct {
	tables    map[string]*TableSchema
	databases map[string]bool
	filepath  string
}

// ── JSON serialization ────────────────────────────────────────

type catalogDisk struct {
	Tables    []tableDisk `json:"tables"`
	Databases []string    `json:"databases"`
}

type tableDisk struct {
	TableName string       `json:"table_name"`
	Columns   []columnDisk `json:"columns"`
}

type columnDisk struct {
	Name       string `json:"name"`
	DataType   int    `json:"data_type"`
	Constraint int    `json:"constraint"`
}

func NewCatalog(filepath string) *Catalog {
	return &Catalog{
		tables:    make(map[string]*TableSchema),
		databases: map[string]bool{"default": true},
		filepath:  filepath,
	}
}

func (c *Catalog) Load() error {
	data, err := os.ReadFile(c.filepath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read catalog: %w", err)
	}
	var disk catalogDisk
	if err := json.Unmarshal(data, &disk); err != nil {
		return err
	}
	for _, t := range disk.Tables {
		var cols []Column
		for _, c := range t.Columns {
			cols = append(cols, Column{
				Name:       c.Name,
				DataType:   DataType(c.DataType),
				Constraint: ColumnConstraint(c.Constraint),
			})
		}
		c.tables[t.TableName] = &TableSchema{TableName: t.TableName, Columns: cols}
	}
	for _, db := range disk.Databases {
		c.databases[db] = true
	}
	fmt.Printf("📂 Loaded %d table(s) from catalog\n", len(c.tables))
	return nil
}

func (c *Catalog) Save() error {
	var disk catalogDisk
	for _, schema := range c.tables {
		var cols []columnDisk
		for _, col := range schema.Columns {
			cols = append(cols, columnDisk{
				Name:       col.Name,
				DataType:   int(col.DataType),
				Constraint: int(col.Constraint),
			})
		}
		disk.Tables = append(disk.Tables, tableDisk{TableName: schema.TableName, Columns: cols})
	}
	for db := range c.databases {
		disk.Databases = append(disk.Databases, db)
	}
	data, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
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
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", name)
	}
	return schema, nil
}

func (c *Catalog) HasTable(name string) bool {
	_, ok := c.tables[name]
	return ok
}

func (c *Catalog) GetAllTables() []*TableSchema {
	var tables []*TableSchema
	for _, t := range c.tables {
		tables = append(tables, t)
	}
	return tables
}

func (c *Catalog) CreateDatabase(name string) error {
	if c.databases[name] {
		return fmt.Errorf("database '%s' already exists", name)
	}
	c.databases[name] = true
	return c.Save()
}

func (c *Catalog) GetAllDatabases() []string {
	var dbs []string
	for db := range c.databases {
		dbs = append(dbs, db)
	}
	return dbs
}