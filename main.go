package main

import (
	"dbengine/catalog"
	"dbengine/execution"
	"dbengine/server"
	sqlpkg "dbengine/sql"
	"dbengine/wal"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println(`
╔══════════════════════════════════════════╗
║         🗄️  DB ENGINE v2.0               ║
║  Storage · B+Tree · SQL · WAL · JOIN     ║
╚══════════════════════════════════════════╝`)

	walLog, err := wal.NewWAL("mydb.wal")
	if err != nil {
		panic(err)
	}
	defer walLog.Close()

	srv := server.NewServer(walLog)

	// load default database
	defaultCat := catalog.NewCatalog("default_catalog.json")
	if err := defaultCat.Load(); err != nil {
		fmt.Printf("⚠️  Catalog error: %s\n", err)
	}
	defaultExec := execution.NewExecutor(defaultCat, "default_")
	fmt.Println("📂 Loading existing tables...")
	defaultExec.LoadExistingTables()
	defer defaultExec.CloseAll()
	srv.AddDatabase("default", defaultCat, defaultExec)

	// load any other databases that were previously created
	entries, _ := os.ReadDir(".")
	for _, entry := range entries {
		name := entry.Name()
		// find all *_catalog.json files that aren't default
		if strings.HasSuffix(name, "_catalog.json") && name != "default_catalog.json" {
			dbName := strings.TrimSuffix(name, "_catalog.json")
			fmt.Printf("📂 Loading database: %s\n", dbName)
			cat := catalog.NewCatalog(name)
			if err := cat.Load(); err != nil {
				continue
			}
			exec := execution.NewExecutor(cat, dbName+"_")
			exec.LoadExistingTables()
			srv.AddDatabase(dbName, cat, exec)
		}
	}

	// seed only if fresh
	if len(defaultCat.GetAllTables()) == 0 {
		fmt.Println("🌱 Seeding demo data...")
		seedDemoData(defaultExec, walLog)
	} else {
		fmt.Println("✅ Existing data loaded")
	}

	srv.Start(":8080")
}

func seedDemoData(executor *execution.Executor, walLog *wal.WAL) {
	seeds := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
		"INSERT INTO users VALUES (1, 'Alice', 25)",
		"INSERT INTO users VALUES (2, 'Bob', 30)",
		"INSERT INTO users VALUES (3, 'Charlie', 22)",
		"INSERT INTO users VALUES (4, 'Diana', 28)",
		"INSERT INTO users VALUES (5, 'Eve', 35)",
		"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, item TEXT, price INT)",
		"INSERT INTO orders VALUES (1, 1, 'Laptop', 999)",
		"INSERT INTO orders VALUES (2, 2, 'Phone', 699)",
		"INSERT INTO orders VALUES (3, 1, 'Mouse', 49)",
		"INSERT INTO orders VALUES (4, 3, 'Keyboard', 129)",
		"INSERT INTO orders VALUES (5, 5, 'Monitor', 399)",
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
