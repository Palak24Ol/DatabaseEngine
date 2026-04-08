package execution

import (
	"bytes"
	"dbengine/catalog"
	"dbengine/index"
	sqlpkg "dbengine/sql"
	"dbengine/storage"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
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
	prefix       string
	indexes      map[string]*index.BPlusTree
}

func NewExecutor(cat *catalog.Catalog, prefix string) *Executor {
	return &Executor{
		catalog:      cat,
		heapFiles:    make(map[string]*storage.HeapFile),
		diskManagers: make(map[string]*storage.DiskManager),
		prefix:       prefix,
		indexes:      make(map[string]*index.BPlusTree),
	}
}

func (e *Executor) LoadExistingTables() {
	for _, schema := range e.catalog.GetAllTables() {
		disk, err := storage.NewDiskManager(e.prefix + schema.TableName + ".db")
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
	case *sqlpkg.ExplainStatement:
		return e.executeExplain(s)
	case *sqlpkg.CreateIndexStatement:
		return e.executeCreateIndex(s)
	case *sqlpkg.DropIndexStatement:
		return e.executeDropIndex(s)
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

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
		col := catalog.Column{Name: c.Name, DataType: dt, Constraint: constraint}
		if c.ForeignKey != nil {
			col.ForeignKey = &catalog.ForeignKey{RefTable: c.ForeignKey.RefTable, RefColumn: c.ForeignKey.RefColumn}
		}
		cols = append(cols, col)
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

func (e *Executor) executeCreateDB(stmt *sqlpkg.CreateDBStatement) (*Result, error) {
	if err := e.catalog.CreateDatabase(stmt.DBName); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Database '%s' created", stmt.DBName)}, nil
}

func (e *Executor) executeShowDB() (*Result, error) {
	var rows []Row
	for _, db := range e.catalog.GetAllDatabases() {
		rows = append(rows, Row{"Database": db})
	}
	return &Result{Columns: []string{"Database"}, Rows: rows}, nil
}

func (e *Executor) executeInsert(stmt *sqlpkg.InsertStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	if len(stmt.Values) != len(schema.Columns) {
		return nil, fmt.Errorf("expected %d values but got %d", len(schema.Columns), len(stmt.Values))
	}
	if err := e.checkConstraints(schema, stmt.Values); err != nil {
		return nil, err
	}
	if err := e.checkForeignKeys(schema, stmt.Values); err != nil {
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
	var violErr error
	heap.Scan(func(rid storage.RID, data []byte) {
		if violErr != nil {
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
				violErr = fmt.Errorf("%s constraint violation: duplicate value '%s' for column '%s'", name, values[c.idx], c.colName)
				return
			}
		}
	})
	return violErr
}

func (e *Executor) checkForeignKeys(schema *catalog.TableSchema, values []string) error {
	for i, col := range schema.Columns {
		if col.ForeignKey == nil {
			continue
		}
		val := values[i]
		refSchema, err := e.catalog.GetTable(col.ForeignKey.RefTable)
		if err != nil {
			continue
		}
		refHeap, err := e.getHeapFile(col.ForeignKey.RefTable)
		if err != nil {
			continue
		}
		found := false
		refHeap.Scan(func(rid storage.RID, data []byte) {
			if found {
				return
			}
			row, err := deserializeRow(refSchema, data)
			if err != nil {
				return
			}
			if row[col.ForeignKey.RefColumn] == val {
				found = true
			}
		})
		if !found {
			return fmt.Errorf("FOREIGN KEY violation: value '%s' not found in %s(%s)", val, col.ForeignKey.RefTable, col.ForeignKey.RefColumn)
		}
	}
	return nil
}

func (e *Executor) executeSelect(stmt *sqlpkg.SelectStatement) (*Result, error) {
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
	resultCols := resolveColumns(stmt.Exprs, allCols)
	var result []Row
	for _, row := range rows {
		projected := make(Row)
		for _, col := range resultCols {
			projected[col] = resolveCol(row, col)
		}
		result = append(result, projected)
	}
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
	if stmt.Limit > 0 && len(result) > stmt.Limit {
		result = result[:stmt.Limit]
	}
	return &Result{Columns: resultCols, Rows: result}, nil
}

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
	var rightRows []Row
	rightHeap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(rightSchema, data)
		if err != nil {
			return
		}
		rightRows = append(rightRows, row)
	})
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
			if leftRow[j.LeftCol] != rightRow[j.RightCol] {
				continue
			}
			merged := make(Row)
			for k, v := range leftRow {
				merged[stmt.TableName+"."+k] = v
				merged[k] = v
			}
			for k, v := range rightRow {
				merged[stmt.Join.TableName+"."+k] = v
				if _, ok := merged[k]; !ok {
					merged[k] = v
				}
			}
			if stmt.Where != nil && !applyFilter(merged, stmt.Where) {
				continue
			}
			result = append(result, merged)
		}
	})
	return result, allCols, nil
}

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

func (e *Executor) executeExplain(stmt *sqlpkg.ExplainStatement) (*Result, error) {
	var steps []Row
	stepNum := 1
	add := func(op, detail, cost string) {
		steps = append(steps, Row{"Step": strconv.Itoa(stepNum), "Operation": op, "Detail": detail, "Est. Cost": cost})
		stepNum++
	}
	switch s := stmt.Inner.(type) {
	case *sqlpkg.SelectStatement:
		schema, err := e.catalog.GetTable(s.TableName)
		if err != nil {
			return nil, err
		}
		hasIndex := false
		if s.Where != nil && !s.Where.IsCompound {
			if e.indexes[s.TableName+"_"+s.Where.Column+"_idx"] != nil {
				hasIndex = true
			}
		}
		add("SCAN TABLE", fmt.Sprintf("table=%s (%d columns)", s.TableName, len(schema.Columns)), "O(n)")
		if s.Where != nil {
			if hasIndex {
				add("INDEX SCAN", fmt.Sprintf("B+ tree index on '%s'", s.Where.Column), "O(log n) ⚡")
			} else {
				add("FILTER", fmt.Sprintf("WHERE %s %s %s", s.Where.Column, s.Where.Operator, s.Where.Value), "O(n)")
			}
		}
		if s.Join != nil {
			add("NESTED LOOP JOIN", fmt.Sprintf("%s ⋈ %s ON %s.%s = %s.%s", s.TableName, s.Join.TableName, s.Join.LeftTable, s.Join.LeftCol, s.Join.RightTable, s.Join.RightCol), "O(n²)")
		}
		hasAgg := false
		for _, expr := range s.Exprs {
			if expr.AggFunc != "" {
				hasAgg = true
				break
			}
		}
		if hasAgg {
			add("AGGREGATE", "compute COUNT/SUM/AVG/MAX/MIN", "O(n)")
		}
		if s.OrderBy != nil {
			dir := "ASC"
			if s.OrderBy.Desc {
				dir = "DESC"
			}
			add("SORT", fmt.Sprintf("ORDER BY %s %s", s.OrderBy.Column, dir), "O(n log n)")
		}
		if s.Limit > 0 {
			add("LIMIT", fmt.Sprintf("return first %d rows", s.Limit), "O(1)")
		}
		cols := "*"
		if len(s.Exprs) > 0 && !s.Exprs[0].Star {
			var cn []string
			for _, expr := range s.Exprs {
				cn = append(cn, expr.DisplayName())
			}
			cols = strings.Join(cn, ", ")
		}
		add("PROJECT", fmt.Sprintf("columns: %s", cols), "O(n)")
	case *sqlpkg.InsertStatement:
		add("CONSTRAINT CHECK", "verify PRIMARY KEY / UNIQUE constraints", "O(n)")
		add("FK CHECK", "verify FOREIGN KEY references", "O(n)")
		add("SERIALIZE ROW", "encode values to binary format", "O(1)")
		add("HEAP INSERT", fmt.Sprintf("append to %s.db", s.TableName), "O(1)")
		add("WAL WRITE", "write INSERT record", "O(1)")
	case *sqlpkg.UpdateStatement:
		add("SCAN TABLE", fmt.Sprintf("scan all rows in %s", s.TableName), "O(n)")
		if s.Where != nil {
			add("FILTER", fmt.Sprintf("WHERE %s %s %s", s.Where.Column, s.Where.Operator, s.Where.Value), "O(n)")
		}
		add("TOMBSTONE DELETE", "mark rows deleted", "O(k)")
		add("HEAP INSERT", "write updated rows", "O(k)")
		add("WAL WRITE", "write UPDATE record", "O(1)")
	case *sqlpkg.DeleteStatement:
		add("SCAN TABLE", fmt.Sprintf("scan all rows in %s", s.TableName), "O(n)")
		if s.Where != nil {
			add("FILTER", fmt.Sprintf("WHERE %s %s %s", s.Where.Column, s.Where.Operator, s.Where.Value), "O(n)")
		}
		add("TOMBSTONE", "mark matching rows deleted", "O(k)")
		add("WAL WRITE", "write DELETE record", "O(1)")
	default:
		add("UNKNOWN", "cannot explain this statement type", "—")
	}
	return &Result{Columns: []string{"Step", "Operation", "Detail", "Est. Cost"}, Rows: steps}, nil
}

func (e *Executor) executeCreateIndex(stmt *sqlpkg.CreateIndexStatement) (*Result, error) {
	schema, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	if _, err := schema.GetColumnIndex(stmt.Column); err != nil {
		return nil, fmt.Errorf("column '%s' not found in table '%s'", stmt.Column, stmt.TableName)
	}
	tree := index.NewBPlusTree()
	heap, err := e.getHeapFile(stmt.TableName)
	if err != nil {
		return nil, err
	}
	rowNum := 0
	heap.Scan(func(rid storage.RID, data []byte) {
		row, err := deserializeRow(schema, data)
		if err != nil {
			return
		}
		if num, err := strconv.Atoi(row[stmt.Column]); err == nil {
			tree.Insert(num, rid.Offset)
		}
		rowNum++
	})
	e.indexes[stmt.TableName+"_"+stmt.Column+"_idx"] = tree
	e.catalog.AddIndex(catalog.IndexInfo{Name: stmt.IndexName, TableName: stmt.TableName, Column: stmt.Column, Unique: stmt.Unique})
	return &Result{Message: fmt.Sprintf("Index '%s' created on %s(%s) — %d rows indexed", stmt.IndexName, stmt.TableName, stmt.Column, rowNum)}, nil
}

func (e *Executor) executeDropIndex(stmt *sqlpkg.DropIndexStatement) (*Result, error) {
	info := e.catalog.GetIndex(stmt.IndexName)
	if info == nil {
		return nil, fmt.Errorf("index '%s' not found", stmt.IndexName)
	}
	delete(e.indexes, info.TableName+"_"+info.Column+"_idx")
	e.catalog.DropIndex(stmt.IndexName)
	return &Result{Message: fmt.Sprintf("Index '%s' dropped", stmt.IndexName)}, nil
}

func applyFilter(row Row, where *sqlpkg.WhereClause) bool {
	if where.IsCompound {
		l := applyFilter(row, where.Left)
		r := applyFilter(row, where.Right)
		if where.Logic == "AND" {
			return l && r
		}
		return l || r
	}
	val := resolveCol(row, where.Column)
	rn, rerr := strconv.Atoi(val)
	wn, werr := strconv.Atoi(where.Value)
	if rerr == nil && werr == nil {
		switch where.Operator {
		case "=":
			return rn == wn
		case ">":
			return rn > wn
		case "<":
			return rn < wn
		case "!=":
			return rn != wn
		case ">=":
			return rn >= wn
		case "<=":
			return rn <= wn
		}
	}
	switch where.Operator {
	case "=":
		return val == where.Value
	case "!=":
		return val != where.Value
	}
	return false
}

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
			row[col.Name] = strconv.FormatUint(binary.LittleEndian.Uint64(valueBytes), 10)
		} else {
			row[col.Name] = string(valueBytes)
		}
	}
	return row, nil
}

func resolveCol(row Row, col string) string {
	if val, ok := row[col]; ok {
		return val
	}
	suffix := "." + col
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v
		}
	}
	return ""
}

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
