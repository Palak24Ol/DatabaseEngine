package main

import (
	"fmt"
	"dbengine/catalog"
	"dbengine/execution"
	"dbengine/server"
	"dbengine/wal"
	sqlpkg "dbengine/sql"
	"strings"
)

func main() {
	fmt.Println(`
╔══════════════════════════════════════════╗
║         🗄️  DB ENGINE v1.0               ║
║     Storage · B+Tree · SQL · WAL         ║
╚══════════════════════════════════════════╝`)

	// load catalog from disk (persistence!)
	cat := catalog.NewCatalog("catalog.json")
	if err := cat.Load(); err != nil {
		fmt.Printf("⚠️  Catalog load error: %s\n", err)
	}

	walLog, err := wal.NewWAL("mydb.wal")
	if err != nil {
		panic(err)
	}
	defer walLog.Close()

	executor := execution.NewExecutor(cat)
	defer executor.CloseAll()

	// load existing tables from disk
	fmt.Println("📂 Loading existing tables...")
	executor.LoadExistingTables()

	// only seed if no tables exist yet
	if len(cat.GetAllTables()) == 0 {
		fmt.Println("🌱 Fresh database — seeding demo data...")
		seedDemoData(executor, walLog)
	} else {
		fmt.Println("✅ Existing data loaded — no seeding needed")
	}

	srv := server.NewServer(executor, walLog, cat)
	srv.Start(":8080")
}

func seedDemoData(executor *execution.Executor, walLog *wal.WAL) {
	seeds := []string{
		"CREATE TABLE users (id INT, name TEXT, age INT)",
		"INSERT INTO users VALUES (1, 'Alice', 25)",
		"INSERT INTO users VALUES (2, 'Bob', 30)",
		"INSERT INTO users VALUES (3, 'Charlie', 22)",
		"INSERT INTO users VALUES (4, 'Diana', 28)",
		"INSERT INTO users VALUES (5, 'Eve', 35)",
		"CREATE TABLE products (id INT, name TEXT, price INT)",
		"INSERT INTO products VALUES (1, 'Laptop', 999)",
		"INSERT INTO products VALUES (2, 'Phone', 699)",
		"INSERT INTO products VALUES (3, 'Tablet', 449)",
	}
	for _, q := range seeds {
		parser := sqlpkg.NewParser(q)
		stmt, err := parser.Parse()
		if err != nil {
			continue
		}
		txID := walLog.Begin()
		executor.Execute(stmt)
		walLog.Commit(txID)
	}
	fmt.Println("✅ Demo data seeded")
}

// needed for server package
var _ = strings.TrimSpace