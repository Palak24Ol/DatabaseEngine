package execution

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"dbengine/catalog"
	"dbengine/storage"
	sqlpkg "dbengine/sql"
)

type Row map[string]string
type Result struct {
	Columns []string
	Rows    []Row
	Message string
}

type Executor struct {
	catalog      *catalog.Catalog
	heapFiles    map[string]*storage.HeapFile
	diskManagers map[string]*storage.DiskManager
}

func NewExecutor(cat *catalog.Catalog) *Executor {
	return &Executor{
		catalog:      cat,
		heapFiles:    make(map[string]*storage.HeapFile),
		diskManagers: make(map[string]*storage.DiskManager),
	}
}

// LoadExistingTables opens heap files for tables already in catalog
func (e *Executor) LoadExistingTables() {
	for _, schema := range e.catalog.GetAllTables() {
		filename := schema.TableName + ".db"
		disk, err := storage.NewDiskManager(filename)
		if err != nil {
			continue
		}
		e.diskManagers[schema.TableName] = disk
		e.heapFiles[schema.TableName] = storage.NewHeapFile(disk)
		fmt.Printf("  📄 Loaded table: %s\n", schema.TableName)
	}
}

func (e *Executor) Execute(stmt sqlpkg.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *sqlpkg.CreateStatement:
		return e.executeCreate(s)
	case *sqlpkg.InsertStatement:
		return e.executeInsert(s)
	case *sqlpkg.SelectStatement:
		return e.executeSelect(s)
	case *sqlpkg.DeleteStatement:
		return e.executeDelete(s)
	case *sqlpkg.UpdateStatement:
		return e.executeUpdate(s)
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

// ── CREATE ────────────────────────────────────────────────────

func (e *Executor) executeCreate(stmt *sqlpkg.CreateStatement) (*Result, error) {
	var cols []catalog.Column
	for _, c := range stmt.Columns {
		dt := catalog.TYPE_TEXT
		if strings.ToUpper(c.DataType) == "INT" {
			dt = catalog.TYPE_INT
		}
		cols = append(cols, catalog.Column{Name: c.Name, DataType: dt})
	}
	schema := &catalog.TableSchema{TableName: stmt.TableName, Columns: cols}
	if err := e.catalog.CreateTable(schema); err != nil {
		return nil, err
	}
	if err := e.initTable(stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table '%s' created with %d column(s)", stmt.TableName, len(cols))}, nil
}

// ── INSERT ────────────────────────────────────────────────────

func (e *Executor) executeInsert(stmt *sqlpkg.InsertStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	if len(stmt.Values) != len(schema.Columns) {
		return nil, fmt.Errorf("expected %d values but got %d", len(schema.Columns), len(stmt.Values))
	}
	data, err := serializeRow(schema, stmt.Values)
	if err != nil {
		return nil, err
	}
	heap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, err
	}
	if _, err := heap.InsertTuple(data); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("1 row inserted into '%s'", stmt.TableName)}, nil
}

// ── SELECT with ORDER BY + LIMIT ──────────────────────────────

func (e *Executor) executeSelect(stmt *sqlpkg.SelectStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	var resultCols []string
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		for _, col := range schema.Columns {
			resultCols = append(resultCols, col.Name)
		}
	} else {
		resultCols = stmt.Columns
		for _, col := range resultCols {
			if _, err := schema.GetColumnIndex(col); err != nil {
				return nil, err
			}
		}
	}

	heap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, err
	}

	var rows []Row
	heap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		if stmt.Where != nil && !applyFilter(row, stmt.Where) {
			return
		}
		projected := make(Row)
		for _, col := range resultCols {
			projected[col] = row[col]
		}
		rows = append(rows, projected)
	})

	// ORDER BY
	if stmt.OrderBy != nil {
		col := stmt.OrderBy.Column
		desc := stmt.OrderBy.Desc
		sort.SliceStable(rows, func(i, j int) bool {
			a, b := rows[i][col], rows[j][col]
			// try numeric sort
			an, aerr := strconv.Atoi(a)
			bn, berr := strconv.Atoi(b)
			if aerr == nil && berr == nil {
				if desc {
					return an > bn
				}
				return an < bn
			}
			// string sort
			if desc {
				return a > b
			}
			return a < b
		})
	}

	// LIMIT
	if stmt.Limit > 0 && len(rows) > stmt.Limit {
		rows = rows[:stmt.Limit]
	}

	return &Result{Columns: resultCols, Rows: rows}, nil
}

// ── UPDATE ────────────────────────────────────────────────────

func (e *Executor) executeUpdate(stmt *sqlpkg.UpdateStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	heap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, err
	}

	updateCount := 0
	type updateJob struct {
		rid     storage.RID
		newData []byte
	}
	var jobs []updateJob

	heap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		if stmt.Where != nil && !applyFilter(row, stmt.Where) {
			return
		}

		// apply SET clauses
		for _, set := range stmt.Sets {
			row[set.Column] = set.Value
		}

		// re-serialize in column order
		var values []string
		for _, col := range schema.Columns {
			values = append(values, row[col.Name])
		}
		newData, err := serializeRow(schema, values)
		if err != nil {
			return
		}
		jobs = append(jobs, updateJob{rid: rid, newData: newData})
	})

	// delete old + insert new (update-in-place)
	for _, job := range jobs {
		if err := heap.DeleteTuple(job.rid); err != nil {
			continue
		}
		heap.InsertTuple(job.newData)
		updateCount++
	}

	return &Result{Message: fmt.Sprintf("%d row(s) updated in '%s'", updateCount, stmt.TableName)}, nil
}

// ── DELETE ────────────────────────────────────────────────────

func (e *Executor) executeDelete(stmt *sqlpkg.DeleteStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	heap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, err
	}

	var toDelete []storage.RID
	heap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		if stmt.Where == nil || applyFilter(row, stmt.Where) {
			toDelete = append(toDelete, rid)
		}
	})

	for _, rid := range toDelete {
		heap.DeleteTuple(rid)
	}

	return &Result{Message: fmt.Sprintf("%d row(s) deleted from '%s'", len(toDelete), stmt.TableName)}, nil
}

// ── WHERE filter — supports AND / OR ─────────────────────────

func applyFilter(row Row, where *sqlpkg.WhereClause) bool {
	if where.IsCompound {
		left := applyFilter(row, where.Left)
		right := applyFilter(row, where.Right)
		if where.Logic == "AND" {
			return left && right
		}
		return left || right
	}

	val, ok := row[where.Column]
	if !ok {
		return false
	}

	rn, rerr := strconv.Atoi(val)
	wn, werr := strconv.Atoi(where.Value)

	if rerr == nil && werr == nil {
		switch where.Operator {
		case "=":  return rn == wn
		case ">":  return rn > wn
		case "<":  return rn < wn
		case "!=": return rn != wn
		case ">=": return rn >= wn
		case "<=": return rn <= wn
		}
	}

	switch where.Operator {
	case "=":  return val == where.Value
	case "!=": return val != where.Value
	}
	return false
}

// ── Serialization ─────────────────────────────────────────────

func serializeRow(schema *catalog.TableSchema, values []string) ([]byte, error) {
	var buf bytes.Buffer
	for i, col := range schema.Columns {
		val := values[i]
		if col.DataType == catalog.TYPE_INT {
			n, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("column '%s' expects INT, got '%s'", col.Name, val)
			}
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(n))
			length := make([]byte, 4)
			binary.LittleEndian.PutUint32(length, 8)
			buf.Write(length)
			buf.Write(b)
		} else {
			b := []byte(val)
			length := make([]byte, 4)
			binary.LittleEndian.PutUint32(length, uint32(len(b)))
			buf.Write(length)
			buf.Write(b)
		}
	}
	return buf.Bytes(), nil
}

func deserializeRow(schema *catalog.TableSchema, data []byte) (Row, error) {
	row := make(Row)
	offset := 0
	for _, col := range schema.Columns {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("corrupt row data")
		}
		length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if offset+length > len(data) {
			return nil, fmt.Errorf("corrupt row data")
		}
		valueBytes := data[offset : offset+length]
		offset += length
		if col.DataType == catalog.TYPE_INT {
			n := binary.LittleEndian.Uint64(valueBytes)
			row[col.Name] = strconv.FormatUint(n, 10)
		} else {
			row[col.Name] = string(valueBytes)
		}
	}
	return row, nil
}

// ── Helpers ───────────────────────────────────────────────────

func (e *Executor) initTable(tableName string) error {
	disk, err := storage.NewDiskManager(tableName + ".db")
	if err != nil {
		return err
	}
	e.diskManagers[tableName] = disk
	e.heapFiles[tableName] = storage.NewHeapFile(disk)
	return nil
}

func (e *Executor) getHeapFile(tableName string) (*storage.HeapFile, error) {
	if heap, ok := e.heapFiles[tableName]; ok {
		return heap, nil
	}
	return nil, fmt.Errorf("no storage for table '%s'", tableName)
}

func (e *Executor) CloseAll() {
	for _, disk := range e.diskManagers {
		disk.Close()
	}
}