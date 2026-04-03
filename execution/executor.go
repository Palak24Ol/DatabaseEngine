package execution

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
	prefix       string // database prefix for file names
}

func NewExecutor(cat *catalog.Catalog, prefix string) *Executor {
	return &Executor{
		catalog:      cat,
		heapFiles:    make(map[string]*storage.HeapFile),
		diskManagers: make(map[string]*storage.DiskManager),
		prefix:       prefix,
	}
}

func (e *Executor) LoadExistingTables() {
	for _, schema := range e.catalog.GetAllTables() {
		filename := e.prefix + schema.TableName + ".db"
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
	case *sqlpkg.CreateDBStatement:
		return e.executeCreateDB(s)
	case *sqlpkg.ShowDBStatement:
		return e.executeShowDB()
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

// ── CREATE TABLE ─────────────────────────────────────────────

func (e *Executor) executeCreate(stmt *sqlpkg.CreateStatement) (*Result, error) {
	var cols []catalog.Column
	for _, c := range stmt.Columns {
		dt := catalog.TYPE_TEXT
		if strings.ToUpper(c.DataType) == "INT" {
			dt = catalog.TYPE_INT
		}
		constraint := catalog.CONSTRAINT_NONE
		if c.PrimaryKey {
			constraint = catalog.CONSTRAINT_PRIMARY_KEY
		} else if c.Unique {
			constraint = catalog.CONSTRAINT_UNIQUE
		}
		cols = append(cols, catalog.Column{Name: c.Name, DataType: dt, Constraint: constraint})
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

// ── CREATE DATABASE ───────────────────────────────────────────

func (e *Executor) executeCreateDB(stmt *sqlpkg.CreateDBStatement) (*Result, error) {
	if err := e.catalog.CreateDatabase(stmt.DBName); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Database '%s' created", stmt.DBName)}, nil
}

// ── SHOW DATABASES ────────────────────────────────────────────

func (e *Executor) executeShowDB() (*Result, error) {
	dbs := e.catalog.GetAllDatabases()
	var rows []Row
	for _, db := range dbs {
		rows = append(rows, Row{"Database": db})
	}
	return &Result{Columns: []string{"Database"}, Rows: rows}, nil
}

// ── INSERT with constraint check ──────────────────────────────

func (e *Executor) executeInsert(stmt *sqlpkg.InsertStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	if len(stmt.Values) != len(schema.Columns) {
		return nil, fmt.Errorf("expected %d values but got %d", len(schema.Columns), len(stmt.Values))
	}

	// check PRIMARY KEY and UNIQUE constraints
	if err := e.checkConstraints(schema, stmt.Values); err != nil {
		return nil, err
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

func (e *Executor) checkConstraints(schema *catalog.TableSchema, values []string) error {
	heap, err := e.getHeapFile(schema.TableName)
	if err != nil {
		return nil
	}

	// find constrained columns
	type check struct {
		idx        int
		colName    string
		constraint catalog.ColumnConstraint
	}
	var checks []check
	for i, col := range schema.Columns {
		if col.Constraint != catalog.CONSTRAINT_NONE {
			checks = append(checks, check{i, col.Name, col.Constraint})
		}
	}
	if len(checks) == 0 {
		return nil
	}

	var violationErr error
	heap.Scan(func(rid storage.RID, data []byte) {
		if violationErr != nil {
			return
		}
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		for _, c := range checks {
			if row[c.colName] == values[c.idx] {
				name := "UNIQUE"
				if c.constraint == catalog.CONSTRAINT_PRIMARY_KEY {
					name = "PRIMARY KEY"
				}
				violationErr = fmt.Errorf("%s constraint violation: duplicate value '%s' for column '%s'",
					name, values[c.idx], c.colName)
				return
			}
		}
	})
	return violationErr
}

// ── SELECT with JOIN + Aggregates ─────────────────────────────

func (e *Executor) executeSelect(stmt *sqlpkg.SelectStatement) (*Result, error) {
	// get rows (with or without JOIN)
	var rows []Row
	var allCols []string

	if stmt.Join != nil {
		var err error
		rows, allCols, err = e.executeJoin(stmt)
		if err != nil {
			return nil, err
		}
	} else {
		schema, err := e.catalog.GetTable(stmt.TableName)
		if err != nil {
			return nil, err
		}
		for _, col := range schema.Columns {
			allCols = append(allCols, col.Name)
		}
		heap, err := e.getHeapFile(stmt.TableName)
		if err != nil {
			return nil, err
		}
		heap.Scan(func(rid storage.RID, data []byte) {
			row, err := deserializeRow(schema, data)
			if err != nil {
				return
			}
			if stmt.Where != nil && !applyFilter(row, stmt.Where) {
				return
			}
			rows = append(rows, row)
		})
	}

	// check if any aggregates
	hasAgg := false
	for _, expr := range stmt.Exprs {
		if expr.AggFunc != "" {
			hasAgg = true
			break
		}
	}

	if hasAgg {
		return e.computeAggregates(stmt.Exprs, rows)
	}

	// resolve columns to project
	resultCols := resolveColumns(stmt.Exprs, allCols)

	// project
	var result []Row
	for _, row := range rows {
		projected := make(Row)
		for _, col := range resultCols {
			projected[col] = resolveCol(row, col)
		}
		result = append(result, projected)
	}

	// ORDER BY
	if stmt.OrderBy != nil {
		col := stmt.OrderBy.Column
		desc := stmt.OrderBy.Desc
		sort.SliceStable(result, func(i, j int) bool {
			a, b := resolveCol(result[i], col), resolveCol(result[j], col)
			an, aerr := strconv.Atoi(a)
			bn, berr := strconv.Atoi(b)
			if aerr == nil && berr == nil {
				if desc {
					return an > bn
				}
				return an < bn
			}
			if desc {
				return a > b
			}
			return a < b
		})
	}

	// LIMIT
	if stmt.Limit > 0 && len(result) > stmt.Limit {
		result = result[:stmt.Limit]
	}

	return &Result{Columns: resultCols, Rows: result}, nil
}

// ── JOIN (Nested Loop) ────────────────────────────────────────

func (e *Executor) executeJoin(stmt *sqlpkg.SelectStatement) ([]Row, []string, error) {
	leftSchema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, nil, err
	}
	rightSchema, err := e.catalog.GetTable(stmt.Join.TableName)
	if err != nil {
		return nil, nil, err
	}

	leftHeap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, nil, err
	}
	rightHeap, err := e.getHeapFile(stmt.Join.TableName)
	if err != nil {
		return nil, nil, err
	}

	// collect all right rows first (for nested loop efficiency)
	var rightRows []Row
	rightHeap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(rightSchema, data)
		if err != nil {
			return
		}
		rightRows = append(rightRows, row)
	})

	// build all column names (qualified)
	var allCols []string
	for _, col := range leftSchema.Columns {
		allCols = append(allCols, stmt.TableName+"."+col.Name)
	}
	for _, col := range rightSchema.Columns {
		allCols = append(allCols, stmt.Join.TableName+"."+col.Name)
	}

	j := stmt.Join
	var result []Row

	leftHeap.Scan(func(rid storage.RID, data []byte) {
		leftRow, err := deserializeRow(leftSchema, data)
		if err != nil {
			return
		}

		for _, rightRow := range rightRows {
			// check ON condition
			leftVal := leftRow[j.LeftCol]
			rightVal := rightRow[j.RightCol]
			if leftVal != rightVal {
				continue
			}

			// merge rows with qualified keys
			merged := make(Row)
			for k, v := range leftRow {
				merged[stmt.TableName+"."+k] = v
				merged[k] = v // also unqualified
			}
			for k, v := range rightRow {
				merged[stmt.Join.TableName+"."+k] = v
				if _, exists := merged[k]; !exists {
					merged[k] = v
				}
			}

			// apply WHERE on merged row
			if stmt.Where != nil && !applyFilter(merged, stmt.Where) {
				continue
			}

			result = append(result, merged)
		}
	})

	return result, allCols, nil
}

// ── Aggregates ────────────────────────────────────────────────

func (e *Executor) computeAggregates(exprs []sqlpkg.SelectExpr, rows []Row) (*Result, error) {
	var resultCols []string
	resultRow := make(Row)

	for _, expr := range exprs {
		if expr.AggFunc == "" {
			continue
		}
		name := expr.DisplayName()
		resultCols = append(resultCols, name)

		switch expr.AggFunc {
		case "COUNT":
			resultRow[name] = strconv.Itoa(len(rows))

		case "SUM":
			sum := 0
			for _, row := range rows {
				if v, err := strconv.Atoi(resolveCol(row, expr.AggArg)); err == nil {
					sum += v
				}
			}
			resultRow[name] = strconv.Itoa(sum)

		case "AVG":
			if len(rows) == 0 {
				resultRow[name] = "0"
				continue
			}
			sum := 0
			for _, row := range rows {
				if v, err := strconv.Atoi(resolveCol(row, expr.AggArg)); err == nil {
					sum += v
				}
			}
			resultRow[name] = fmt.Sprintf("%.2f", float64(sum)/float64(len(rows)))

		case "MAX":
			max := math.MinInt64
			for _, row := range rows {
				if v, err := strconv.Atoi(resolveCol(row, expr.AggArg)); err == nil && v > max {
					max = v
				}
			}
			if max == math.MinInt64 {
				resultRow[name] = "NULL"
			} else {
				resultRow[name] = strconv.Itoa(max)
			}

		case "MIN":
			min := math.MaxInt64
			for _, row := range rows {
				if v, err := strconv.Atoi(resolveCol(row, expr.AggArg)); err == nil && v < min {
					min = v
				}
			}
			if min == math.MaxInt64 {
				resultRow[name] = "NULL"
			} else {
				resultRow[name] = strconv.Itoa(min)
			}
		}
	}

	return &Result{Columns: resultCols, Rows: []Row{resultRow}}, nil
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

	type job struct {
		rid     storage.RID
		newData []byte
	}
	var jobs []job

	heap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		if stmt.Where != nil && !applyFilter(row, stmt.Where) {
			return
		}
		for _, set := range stmt.Sets {
			row[set.Column] = set.Value
		}
		var values []string
		for _, col := range schema.Columns {
			values = append(values, row[col.Name])
		}
		newData, err := serializeRow(schema, values)
		if err != nil {
			return
		}
		jobs = append(jobs, job{rid: rid, newData: newData})
	})

	for _, j := range jobs {
		heap.DeleteTuple(j.rid)
		heap.InsertTuple(j.newData)
	}

	return &Result{Message: fmt.Sprintf("%d row(s) updated in '%s'", len(jobs), stmt.TableName)}, nil
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

// ── WHERE filter with AND/OR + qualified cols ─────────────────

func applyFilter(row Row, where *sqlpkg.WhereClause) bool {
	if where.IsCompound {
		left := applyFilter(row, where.Left)
		right := applyFilter(row, where.Right)
		if where.Logic == "AND" {
			return left && right
		}
		return left || right
	}

	val := resolveCol(row, where.Column)

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

// resolveCol handles both "col" and "table.col" lookups
func resolveCol(row Row, col string) string {
	if val, ok := row[col]; ok {
		return val
	}
	// try qualified lookup: find any key ending in ".col"
	suffix := "." + col
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v
		}
	}
	return ""
}

// resolveColumns expands * and returns final column list
func resolveColumns(exprs []sqlpkg.SelectExpr, allCols []string) []string {
	if len(exprs) == 1 && exprs[0].Star {
		return allCols
	}
	var cols []string
	for _, expr := range exprs {
		if expr.Star {
			cols = append(cols, allCols...)
		} else {
			cols = append(cols, expr.DisplayName())
		}
	}
	return cols
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
	disk, err := storage.NewDiskManager(e.prefix + tableName + ".db")
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