package server

import (
	"dbengine/catalog"
	"dbengine/execution"
	sqlpkg "dbengine/sql"
	"dbengine/wal"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type dbContext struct {
	catalog  *catalog.Catalog
	executor *execution.Executor
}

type Server struct {
	mu        sync.Mutex
	databases map[string]*dbContext
	currentDB string
	wal       *wal.WAL
}

func NewServer(walLog *wal.WAL) *Server {
	return &Server{
		databases: make(map[string]*dbContext),
		currentDB: "default",
		wal:       walLog,
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
	mux.HandleFunc("/api/query", s.handleQuery)
	mux.HandleFunc("/api/wal", s.handleWAL)
	mux.HandleFunc("/api/tables", s.handleTables)
	mux.HandleFunc("/api/databases", s.handleDatabases)
	mux.HandleFunc("/api/use", s.handleUse)
	fmt.Printf("🌐 Web UI running at http://localhost%s\n", port)
	return http.ListenAndServe(port, corsMiddleware(mux))
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

	parser := sqlpkg.NewParser(req.SQL)
	stmt, err := parser.Parse()
	if err != nil {
		writeJSON(w, QueryResponse{Error: fmt.Sprintf("Parse error: %s", err)})
		return
	}

	// handle USE DATABASE
	if useStmt, ok := stmt.(*sqlpkg.UseDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.databases[useStmt.DBName]; !exists {
			writeJSON(w, QueryResponse{Error: fmt.Sprintf("database '%s' does not exist", useStmt.DBName)})
			return
		}
		s.currentDB = useStmt.DBName
		writeJSON(w, QueryResponse{Message: fmt.Sprintf("Switched to database '%s'", useStmt.DBName)})
		return
	}

	// handle CREATE DATABASE — create executor context immediately
	if createStmt, ok := stmt.(*sqlpkg.CreateDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.databases[createStmt.DBName]; exists {
			writeJSON(w, QueryResponse{Error: fmt.Sprintf("database '%s' already exists", createStmt.DBName)})
			return
		}
		ctx, err := s.createDBContext(createStmt.DBName)
		if err != nil {
			writeJSON(w, QueryResponse{Error: err.Error()})
			return
		}
		s.databases[createStmt.DBName] = ctx
		writeJSON(w, QueryResponse{Message: fmt.Sprintf("Database '%s' created", createStmt.DBName)})
		return
	}

	// handle SHOW DATABASES at server level — knows ALL databases
	if _, ok := stmt.(*sqlpkg.ShowDBStatement); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		type row struct {
			Database string `json:"Database"`
		}
		var rows []map[string]string
		for name := range s.databases {
			rows = append(rows, map[string]string{"Database": name})
		}
		writeJSON(w, QueryResponse{
			Columns: []string{"Database"},
			Rows:    rows,
		})
		return
	}

	s.mu.Lock()
	ctx := s.currentContext()
	s.mu.Unlock()

	txID := s.wal.Begin()
	result, err := ctx.executor.Execute(stmt)
	if err != nil {
		s.wal.Abort(txID)
		writeJSON(w, QueryResponse{Error: fmt.Sprintf("Error: %s", err)})
		return
	}
	s.wal.Commit(txID)

	var rows []map[string]string
	for _, row := range result.Rows {
		rows = append(rows, map[string]string(row))
	}
	writeJSON(w, QueryResponse{
		Columns: result.Columns,
		Rows:    rows,
		Message: result.Message,
	})
}

// createDBContext creates a fresh catalog + executor for a new database
func (s *Server) createDBContext(name string) (*dbContext, error) {
	cat := catalog.NewCatalog(name + "_catalog.json")
	if err := cat.Load(); err != nil {
		return nil, err
	}
	exec := execution.NewExecutor(cat, name+"_")
	exec.LoadExistingTables()
	return &dbContext{catalog: cat, executor: exec}, nil
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

// ── Helpers ───────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
}
