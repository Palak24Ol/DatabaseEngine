package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"dbengine/catalog"
	"dbengine/execution"
	sqlpkg "dbengine/sql"
	"dbengine/wal"
)

type Server struct {
	executor *execution.Executor
	wal      *wal.WAL
	catalog  *catalog.Catalog
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Columns []string            `json:"columns"`
	Rows    []map[string]string `json:"rows"`
	Message string              `json:"message"`
	Error   string              `json:"error,omitempty"`
}

func NewServer(executor *execution.Executor, walLog *wal.WAL, cat *catalog.Catalog) *Server {
	return &Server{executor: executor, wal: walLog, catalog: cat}
}

func (s *Server) Start(port string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/query", s.handleQuery)
	mux.HandleFunc("/api/wal", s.handleWAL)
	mux.HandleFunc("/api/tables", s.handleTables) // ✅ added

	handler := corsMiddleware(mux)
	fmt.Printf("🌐 Web UI running at http://localhost%s\n", port)
	return http.ListenAndServe(port, handler)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, QueryResponse{Error: "invalid request body"})
		return
	}

	if req.SQL == "" {
		writeJSON(w, QueryResponse{Error: "empty query"})
		return
	}

	parser := sqlpkg.NewParser(req.SQL)
	stmt, err := parser.Parse()
	if err != nil {
		writeJSON(w, QueryResponse{Error: fmt.Sprintf("Parse error: %s", err)})
		return
	}

	txID := s.wal.Begin()
	result, err := s.executor.Execute(stmt)
	if err != nil {
		s.wal.Abort(txID)
		writeJSON(w, QueryResponse{Error: fmt.Sprintf("Execution error: %s", err)})
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

type WALEntry struct {
	LSN   uint64 `json:"lsn"`
	TxID  uint64 `json:"txid"`
	Type  string `json:"type"`
	Table string `json:"table"`
}

func (s *Server) handleWAL(w http.ResponseWriter, r *http.Request) {
	records, err := s.wal.ReadAll()
	if err != nil {
		writeJSON(w, map[string]string{"error": err.Error()})
		return
	}

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
		entries = append(entries, WALEntry{
			LSN:   rec.LSN,
			TxID:  rec.TxID,
			Type:  typeName,
			Table: rec.TableName,
		})
	}
	writeJSON(w, entries)
}

// ✅ NEW ENDPOINT
func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	tables := s.catalog.GetAllTables()

	type colInfo struct {
		Name     string `json:"name"`
		DataType string `json:"type"`
	}
	type tableInfo struct {
		Name    string    `json:"name"`
		Columns []colInfo `json:"columns"`
	}

	var result []tableInfo

	for _, t := range tables {
		var cols []colInfo
		for _, c := range t.Columns {
			dt := "TEXT"
			if c.DataType == 0 {
				dt = "INT"
			}
			cols = append(cols, colInfo{
				Name:     c.Name,
				DataType: dt,
			})
		}
		result = append(result, tableInfo{
			Name:    t.TableName,
			Columns: cols,
		})
	}

	writeJSON(w, result)
}

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
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}