package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"dbengine/catalog"
	"dbengine/execution"
	"dbengine/wal"
	sqlpkg "dbengine/sql"
)

type dbContext struct {
	catalog  *catalog.Catalog
	executor *execution.Executor
}

// SlowQuery stores a slow query log entry
type SlowQuery struct {
	SQL      string    `json:"sql"`
	Duration float64   `json:"duration_ms"`
	Time     time.Time `json:"time"`
}

// QueryStat tracks aggregate stats
type QueryStat struct {
	Count   int     `json:"count"`
	TotalMs float64 `json:"total_ms"`
	SlowMs  float64 `json:"slow_threshold_ms"`
}

type Server struct {
	mu          sync.Mutex
	databases   map[string]*dbContext
	currentDB   string
	wal         *wal.WAL
	slowQueries []SlowQuery
	queryStat   QueryStat
	// auth
	authEnabled  bool
	authUser     string
	authPassword string
}

func NewServer(walLog *wal.WAL) *Server {
	return &Server{
		databases:    make(map[string]*dbContext),
		currentDB:    "default",
		wal:          walLog,
		queryStat:    QueryStat{SlowMs: 200}, // flag queries over 200ms
		authEnabled:  true,
		authUser:     "admin",
		authPassword: "dbengine123",
	}
}

func (s *Server) AddDatabase(name string, cat *catalog.Catalog, exec *execution.Executor) {
	s.databases[name] = &dbContext{catalog: cat, executor: exec}
}

func (s *Server) currentContext() *dbContext {
	return s.databases[s.currentDB]
}

func (s *Server) Start(port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/query",       s.handleQuery)
	mux.HandleFunc("/api/wal",         s.handleWAL)
	mux.HandleFunc("/api/tables",      s.handleTables)
	mux.HandleFunc("/api/databases",   s.handleDatabases)
	mux.HandleFunc("/api/use",         s.handleUse)
	mux.HandleFunc("/api/schema",      s.handleSchema)
	mux.HandleFunc("/api/indexes",     s.handleIndexes)
	mux.HandleFunc("/api/metrics",     s.handleMetrics)
	mux.HandleFunc("/api/slow-queries",s.handleSlowQueries)
	mux.HandleFunc("/api/auth",        s.handleAuth)
	mux.HandleFunc("/api/multi-query", s.handleMultiQuery)
	fmt.Printf("🌐 Web UI running at http://localhost%s\n", port)
	return http.ListenAndServe(port, corsMiddleware(mux))
}

// ── /api/auth ─────────────────────────────────────────────────

type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AuthResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, AuthResponse{OK: false, Message: "method not allowed"})
		return
	}
	var req AuthRequest
	json.NewDecoder(r.Body).Decode(&req)
	if req.Username == s.authUser && req.Password == s.authPassword {
		writeJSON(w, AuthResponse{OK: true, Message: "authenticated"})
	} else {
		writeJSON(w, AuthResponse{OK: false, Message: "invalid credentials"})
	}
}

// ── /api/query ────────────────────────────────────────────────

type QueryRequest struct {
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Columns []string            `json:"columns"`
	Rows    []map[string]string `json:"rows"`
	Message string              `json:"message"`
	Error   string              `json:"error,omitempty"`
}

func (s *Server) executeSQL(sql string) (*QueryResponse, float64) {
	start := time.Now()
	parser := sqlpkg.NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		return &QueryResponse{Error: fmt.Sprintf("Parse error: %s", err)}, 0
	}

	// USE DATABASE
	if useStmt, ok := stmt.(*sqlpkg.UseDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.databases[useStmt.DBName]; !exists {
			return &QueryResponse{Error: fmt.Sprintf("database '%s' does not exist", useStmt.DBName)}, 0
		}
		s.currentDB = useStmt.DBName
		return &QueryResponse{Message: fmt.Sprintf("Switched to database '%s'", useStmt.DBName)}, 0
	}

	// SHOW DATABASES
	if _, ok := stmt.(*sqlpkg.ShowDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		var rows []map[string]string
		for name := range s.databases {
			rows = append(rows, map[string]string{"Database": name})
		}
		return &QueryResponse{Columns: []string{"Database"}, Rows: rows}, 0
	}

	// CREATE DATABASE
	if createStmt, ok := stmt.(*sqlpkg.CreateDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.databases[createStmt.DBName]; exists {
			return &QueryResponse{Error: fmt.Sprintf("database '%s' already exists", createStmt.DBName)}, 0
		}
		ctx, err := s.createDBContext(createStmt.DBName)
		if err != nil {
			return &QueryResponse{Error: err.Error()}, 0
		}
		s.databases[createStmt.DBName] = ctx
		return &QueryResponse{Message: fmt.Sprintf("Database '%s' created", createStmt.DBName)}, 0
	}

	s.mu.Lock()
	ctx := s.currentContext()
	s.mu.Unlock()

	txID := s.wal.Begin()
	result, err := ctx.executor.Execute(stmt)
	elapsed := time.Since(start).Seconds() * 1000

	if err != nil {
		s.wal.Abort(txID)
		return &QueryResponse{Error: fmt.Sprintf("Error: %s", err)}, elapsed
	}

	switch sv := stmt.(type) {
	case *sqlpkg.InsertStatement:
		s.wal.LogInsert(txID, sv.TableName, nil)
	case *sqlpkg.DeleteStatement:
		s.wal.LogDelete(txID, sv.TableName, nil)
	}
	s.wal.Commit(txID)

	var rows []map[string]string
	for _, row := range result.Rows {
		rows = append(rows, map[string]string(row))
	}

	return &QueryResponse{
		Columns: result.Columns,
		Rows:    rows,
		Message: result.Message,
	}, elapsed
}

func (s *Server) recordStats(sql string, ms float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryStat.Count++
	s.queryStat.TotalMs += ms
	if ms >= s.queryStat.SlowMs {
		s.slowQueries = append(s.slowQueries, SlowQuery{
			SQL:      sql,
			Duration: ms,
			Time:     time.Now(),
		})
		if len(s.slowQueries) > 100 {
			s.slowQueries = s.slowQueries[1:]
		}
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, QueryResponse{Error: "invalid request"})
		return
	}
	resp, ms := s.executeSQL(req.SQL)
	s.recordStats(req.SQL, ms)
	writeJSON(w, resp)
}

// ── /api/multi-query ──────────────────────────────────────────
// Accepts multiple semicolon-separated SQL statements

type MultiQueryRequest struct {
	SQL string `json:"sql"`
}

type MultiQueryResult struct {
	SQL     string       `json:"sql"`
	Result  *QueryResponse `json:"result"`
	DurationMs float64   `json:"duration_ms"`
}

func (s *Server) handleMultiQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req MultiQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, []interface{}{})
		return
	}

	// split by semicolon
	statements := splitStatements(req.SQL)
	var results []MultiQueryResult

	for _, sql := range statements {
		if sql == "" {
			continue
		}
		resp, ms := s.executeSQL(sql)
		s.recordStats(sql, ms)
		results = append(results, MultiQueryResult{
			SQL:        sql,
			Result:     resp,
			DurationMs: ms,
		})
	}

	// refresh tables if schema changed
	writeJSON(w, results)
}

// splitStatements splits SQL by semicolons, respecting quoted strings
func splitStatements(sql string) []string {
	var statements []string
	var current []rune
	inQuote := false

	for _, ch := range sql {
		if ch == '\'' {
			inQuote = !inQuote
			current = append(current, ch)
		} else if ch == ';' && !inQuote {
			stmt := trimStatement(string(current))
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current = nil
		} else {
			current = append(current, ch)
		}
	}

	// last statement without trailing semicolon
	if stmt := trimStatement(string(current)); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

func trimStatement(s string) string {
	result := ""
	for _, ch := range s {
		if ch != '\n' && ch != '\r' && ch != '\t' {
			result += string(ch)
		} else {
			result += " "
		}
	}
	// trim leading/trailing spaces
	start, end := 0, len(result)-1
	for start <= end && result[start] == ' ' {
		start++
	}
	for end >= start && result[end] == ' ' {
		end--
	}
	if start > end {
		return ""
	}
	return result[start : end+1]
}

// ── /api/metrics ──────────────────────────────────────────────

type Metrics struct {
	TotalTables    int     `json:"total_tables"`
	TotalIndexes   int     `json:"total_indexes"`
	TotalDatabases int     `json:"total_databases"`
	CurrentDB      string  `json:"current_db"`
	WALSize        int     `json:"wal_size"`
	TotalQueries   int     `json:"total_queries"`
	AvgQueryMs     float64 `json:"avg_query_ms"`
	SlowQueries    int     `json:"slow_queries"`
	SlowThresholdMs float64 `json:"slow_threshold_ms"`
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	ctx := s.currentContext()
	currentDB := s.currentDB
	totalDBs := len(s.databases)
	totalQ := s.queryStat.Count
	totalMs := s.queryStat.TotalMs
	slowCount := len(s.slowQueries)
	slowMs := s.queryStat.SlowMs
	s.mu.Unlock()

	records, _ := s.wal.ReadAll()
	avgMs := 0.0
	if totalQ > 0 {
		avgMs = totalMs / float64(totalQ)
	}

	writeJSON(w, Metrics{
		TotalTables:     len(ctx.catalog.GetAllTables()),
		TotalIndexes:    len(ctx.catalog.GetAllIndexes()),
		TotalDatabases:  totalDBs,
		CurrentDB:       currentDB,
		WALSize:         len(records),
		TotalQueries:    totalQ,
		AvgQueryMs:      avgMs,
		SlowQueries:     slowCount,
		SlowThresholdMs: slowMs,
	})
}

// ── /api/slow-queries ─────────────────────────────────────────

func (s *Server) handleSlowQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		s.mu.Lock()
		s.slowQueries = nil
		s.mu.Unlock()
		writeJSON(w, map[string]string{"message": "slow query log cleared"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, s.slowQueries)
}

// ── /api/tables ───────────────────────────────────────────────

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	ctx := s.currentContext()
	s.mu.Unlock()

	type colInfo struct {
		Name       string `json:"name"`
		DataType   string `json:"type"`
		Constraint string `json:"constraint"`
	}
	type tableInfo struct {
		Name    string    `json:"name"`
		Columns []colInfo `json:"columns"`
	}

	var result []tableInfo
	for _, t := range ctx.catalog.GetAllTables() {
		var cols []colInfo
		for _, c := range t.Columns {
			dt := "TEXT"
			if c.DataType == 0 {
				dt = "INT"
			}
			constraint := ""
			switch c.Constraint {
			case 1:
				constraint = "PRIMARY KEY"
			case 2:
				constraint = "UNIQUE"
			}
			cols = append(cols, colInfo{Name: c.Name, DataType: dt, Constraint: constraint})
		}
		result = append(result, tableInfo{Name: t.TableName, Columns: cols})
	}
	writeJSON(w, result)
}

// ── /api/databases ────────────────────────────────────────────

func (s *Server) handleDatabases(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type dbInfo struct {
		Name    string `json:"name"`
		Current bool   `json:"current"`
	}
	var result []dbInfo
	for name := range s.databases {
		result = append(result, dbInfo{Name: name, Current: name == s.currentDB})
	}
	writeJSON(w, result)
}

// ── /api/use ──────────────────────────────────────────────────

func (s *Server) handleUse(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.databases[req.Name]; !exists {
		writeJSON(w, map[string]string{"error": fmt.Sprintf("database '%s' not found", req.Name)})
		return
	}
	s.currentDB = req.Name
	writeJSON(w, map[string]string{"message": fmt.Sprintf("Switched to '%s'", req.Name)})
}

// ── /api/schema ───────────────────────────────────────────────

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	ctx := s.currentContext()
	s.mu.Unlock()

	type fkInfo struct {
		RefTable  string `json:"ref_table"`
		RefColumn string `json:"ref_column"`
	}
	type colInfo struct {
		Name       string  `json:"name"`
		DataType   string  `json:"type"`
		Constraint string  `json:"constraint"`
		FK         *fkInfo `json:"fk,omitempty"`
	}
	type tableInfo struct {
		Name    string    `json:"name"`
		Columns []colInfo `json:"columns"`
	}

	var tables []tableInfo
	for _, t := range ctx.catalog.GetAllTables() {
		var cols []colInfo
		for _, c := range t.Columns {
			dt := "TEXT"
			if c.DataType == 0 {
				dt = "INT"
			}
			constraint := ""
			switch c.Constraint {
			case 1:
				constraint = "PRIMARY KEY"
			case 2:
				constraint = "UNIQUE"
			}
			col := colInfo{Name: c.Name, DataType: dt, Constraint: constraint}
			if c.ForeignKey != nil {
				col.FK = &fkInfo{RefTable: c.ForeignKey.RefTable, RefColumn: c.ForeignKey.RefColumn}
			}
			cols = append(cols, col)
		}
		tables = append(tables, tableInfo{Name: t.TableName, Columns: cols})
	}
	writeJSON(w, tables)
}

// ── /api/indexes ──────────────────────────────────────────────

func (s *Server) handleIndexes(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	ctx := s.currentContext()
	s.mu.Unlock()
	writeJSON(w, ctx.catalog.GetAllIndexes())
}

// ── /api/wal ──────────────────────────────────────────────────

type WALEntry struct {
	LSN   uint64 `json:"lsn"`
	TxID  uint64 `json:"txid"`
	Type  string `json:"type"`
	Table string `json:"table"`
}

func (s *Server) handleWAL(w http.ResponseWriter, r *http.Request) {
	records, _ := s.wal.ReadAll()
	var entries []WALEntry
	for _, rec := range records {
		typeName := ""
		switch rec.Type {
		case 0:
			typeName = "BEGIN"
		case 1:
			typeName = "INSERT"
		case 2:
			typeName = "DELETE"
		case 3:
			typeName = "COMMIT"
		case 4:
			typeName = "ABORT"
		}
		entries = append(entries, WALEntry{LSN: rec.LSN, TxID: rec.TxID, Type: typeName, Table: rec.TableName})
	}
	writeJSON(w, entries)
}

// ── createDBContext ───────────────────────────────────────────

func (s *Server) createDBContext(name string) (*dbContext, error) {
	cat := catalog.NewCatalog(name + "_catalog.json")
	if err := cat.Load(); err != nil {
		return nil, err
	}
	exec := execution.NewExecutor(cat, name+"_")
	exec.LoadExistingTables()
	return &dbContext{catalog: cat, executor: exec}, nil
}

// ── helpers ───────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
}