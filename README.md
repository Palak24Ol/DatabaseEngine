# 🗄️ DB Engine — Built From Scratch in Go

> A fully functional relational database engine built from the ground up — no external database libraries. Implements storage, indexing, SQL parsing, query execution, transactions, and a web GUI.

![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat&logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat)
![Lines](https://img.shields.io/badge/Lines%20of%20Code-5000%2B-blue?style=flat)

---

## 📸 Preview

> Full-featured web GUI — multi-tab SQL editor, live metrics, ER diagram, index manager, WAL log

```
╔══════════════════════════════════════════╗
║         🗄️  DB ENGINE v3.0               ║
║  Storage · B+Tree · SQL · WAL · JOIN     ║
╚══════════════════════════════════════════╝
```

---

## 🚀 What This Is

Most developers use databases. This project **builds one**.

DB Engine is a relational database management system written entirely in Go — no SQLite, no Postgres under the hood. Every component — from the byte layout of pages on disk to the recursive descent SQL parser to the B+ tree index — is implemented from scratch.

This project covers the same concepts taught in CMU's 15-445 Database Systems course, implemented as a working system.

---

## ✨ Features

### 🔧 Storage Engine
- **Page-based disk storage** — fixed 64KB pages, same architecture as PostgreSQL
- **Buffer Pool Manager** — LRU cache of pages in memory, dirty page tracking
- **Heap File** — unordered row storage with tombstone-based deletion
- **Disk Manager** — direct file I/O with page-level read/write operations
- **Per-table `.db` files** — each table gets its own dedicated storage file

### 🌳 Indexing
- **B+ Tree** from scratch — insert, search, delete, range scan
- **Leaf node linked list** — enables O(log n) range scans
- **Index Manager UI** — create and drop indexes from the web interface
- **EXPLAIN support** — shows whether a query uses an index scan or full scan

### 🧠 SQL Engine
- **Hand-written Lexer** — tokenizes SQL strings character by character
- **Recursive Descent Parser** — builds a full Abstract Syntax Tree
- **Query Executor** — walks the AST and executes against real storage
- **Supported statements:**
  ```sql
  SELECT * FROM table WHERE col > val ORDER BY col DESC LIMIT 10
  SELECT t1.col, t2.col FROM t1 JOIN t2 ON t1.id = t2.fk_id
  SELECT COUNT(*), AVG(age), MAX(age), MIN(age), SUM(price) FROM table
  INSERT INTO table VALUES (1, 'Alice', 25)
  UPDATE table SET col = val WHERE id = 1
  DELETE FROM table WHERE id = 1
  CREATE TABLE table (id INT PRIMARY KEY, name TEXT, age INT)
  CREATE DATABASE mydb
  USE mydb
  SHOW DATABASES
  EXPLAIN SELECT * FROM table WHERE id = 1
  CREATE INDEX idx_name ON table(col)
  DROP INDEX idx_name
  ```

### ⚡ Multi-Statement Execution
- Run multiple SQL statements separated by `;` in one shot
- Each result shown in its own block with individual timing
- Respects quoted strings — semicolons inside `'...'` are not treated as delimiters

### 🔒 Transactions & Crash Recovery
- **Write-Ahead Log (WAL)** — every operation logged before execution
- **COMMIT / ABORT** — full transaction support
- **REDO logging** — database recoverable after crash
- **Tombstone deletion** — soft deletes with dead tuple reclamation

### 🔗 Constraints & Integrity
- **PRIMARY KEY** — duplicate rejection on insert
- **UNIQUE** — unique value enforcement per column
- **FOREIGN KEY** — referential integrity with `REFERENCES` syntax
- **AND / OR** — compound WHERE conditions
- **>=, <=, !=** — full comparison operator set

### 💾 Persistence
- Catalog saved to `catalog.json` — tables and indexes survive restarts
- All data written to `.db` files — no in-memory-only state
- Multiple databases — each with isolated storage and catalog

### 🌐 Web GUI
- **Multi-tab SQL editor** — each tab has its own query state
- **Table Designer** — create tables visually without writing SQL
- **Inline Row Editor** — double-click any cell to edit it in place
- **CSV Import** — drag and drop any CSV file, auto-detects types
- **CSV Export** — download any query result as a CSV file
- **Query History** — last 50 queries saved across sessions
- **ER Diagram** — visual schema with foreign key relationships
- **Index Manager** — create/drop B+ tree indexes from UI
- **WAL Log Viewer** — color-coded transaction log
- **Live Metrics Dashboard** — tables, indexes, queries, avg time, slow queries
- **Slow Query Log** — automatically flags queries over 200ms
- **Dark / Light theme** — persistent across sessions
- **Pagination** — handles large datasets (50/100/250/500 rows per page)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Web GUI (HTML/JS)                  │
│         Multi-tab Editor · ER Diagram · Metrics        │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP (REST API)
┌────────────────────────▼────────────────────────────────┐
│                   HTTP Server (Go)                      │
│     /api/query · /api/multi-query · /api/metrics       │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                   SQL Frontend                          │
│         Lexer → Parser → AST → Executor                │
└──────┬──────────────────┬───────────────────────────────┘
       │                  │
┌──────▼──────┐    ┌──────▼──────────────────────────────┐
│  Catalog    │    │           Execution Engine           │
│  (JSON)     │    │  SELECT · INSERT · UPDATE · DELETE  │
│  Schemas    │    │  JOIN · Aggregates · ORDER BY        │
│  Indexes    │    └──────┬──────────────────────────────┘
└─────────────┘           │
                  ┌───────▼──────────────────────────────┐
                  │         Storage Layer                 │
                  │  Buffer Pool (LRU) · Heap File       │
                  │  B+ Tree Index · WAL                 │
                  └───────┬──────────────────────────────┘
                          │
                  ┌───────▼──────────────────────────────┐
                  │           Disk Manager                │
                  │   table.db files · catalog.json      │
                  │   mydb.wal · 64KB pages              │
                  └──────────────────────────────────────┘
```

---

## 📁 Project Structure

```
dbengine/
├── main.go                 # Entry point, server setup, demo seeding
├── catalog/
│   └── catalog.go          # Table schemas, index metadata, persistence
├── storage/
│   ├── page.go             # 64KB page format, tuple insertion
│   ├── disk_manager.go     # Read/write pages to .db files
│   ├── heap_file.go        # Unordered row storage, tombstone deletes
│   └── buffer_pool.go      # LRU page cache
├── index/
│   └── btree.go            # B+ tree: insert, search, delete, range scan
├── sql/
│   ├── ast.go              # AST node definitions for all statements
│   ├── lexer.go            # Tokenizer — SQL string → token stream
│   └── parser.go           # Recursive descent parser → AST
├── execution/
│   └── executor.go         # Executes AST against storage layer
├── wal/
│   └── wal.go              # Write-ahead log, COMMIT/ABORT, crash recovery
├── server/
│   └── server.go           # HTTP API, multi-query, metrics, slow log
└── frontend_s5.html        # Complete web GUI — no framework
```

---

## ⚙️ Getting Started

### Prerequisites
- Go 1.21+

### Run

```bash
git clone https://github.com/YOUR_USERNAME/db-engine
cd db-engine
go run main.go
```

Open your browser at:
```
http://localhost:8080
```

Demo data is automatically seeded on first run:
- `users` table — 5 rows with id, name, age
- `orders` table — 5 rows with id, user_id, item, price

---

## 💡 Example Queries

```sql
-- Basic SELECT with filter and sort
SELECT * FROM users WHERE age > 25 ORDER BY age DESC;

-- JOIN two tables
SELECT users.name, orders.item, orders.price
FROM users JOIN orders ON users.id = orders.user_id;

-- Aggregate functions
SELECT COUNT(*), AVG(age), MAX(age), MIN(age) FROM users;

-- Multi-statement execution
SELECT * FROM users;
SELECT COUNT(*) FROM orders;
SELECT SUM(price) FROM orders;

-- Create a table with constraints
CREATE TABLE products (
    id   INT PRIMARY KEY,
    name TEXT UNIQUE,
    price INT
);

-- Create an index
CREATE INDEX idx_age ON users(age);

-- Explain a query plan
EXPLAIN SELECT * FROM users WHERE age > 25;

-- Multiple databases
CREATE DATABASE shop;
USE shop;
SHOW DATABASES;
```

---

## 🔬 How Key Components Work

### Page Layout
Every row is stored in a 64KB page. The page header tracks the number of slots and the free space offset. Each tuple is prefixed with a 1-byte alive flag and a 4-byte length — enabling O(1) tombstone deletes without rewriting the page.

### B+ Tree
The B+ tree uses order-4 nodes. Internal nodes hold keys and child pointers. Leaf nodes hold key-value pairs and are connected in a linked list — this is what makes range scans efficient. When a node overflows, it splits and propagates the middle key upward.

### SQL Parser
The lexer converts raw SQL into a flat token stream. The recursive descent parser consumes tokens and builds a typed AST — `SelectStatement`, `InsertStatement`, etc. The executor then pattern-matches on the AST type and dispatches to the correct handler.

### WAL
Before any data is written to disk, a log record is appended to `mydb.wal`. Each record contains the LSN, transaction ID, operation type, and table name. On crash recovery, committed transactions are redone and aborted ones are skipped.

---

## 📊 Performance Characteristics

| Operation | Complexity | Notes |
|---|---|---|
| Full table scan | O(n) | Sequential heap file read |
| B+ tree point lookup | O(log n) | Index scan |
| B+ tree range scan | O(log n + k) | Leaf linked list traversal |
| Insert | O(1) amortized | Append to heap |
| Delete | O(n) | Scan + tombstone |
| Nested loop join | O(n²) | Both tables scanned |
| Sort (ORDER BY) | O(n log n) | In-memory sort |

---

## 🛣️ What I Learned

Building this project required understanding:

- How databases actually store data on disk at the byte level
- Why B+ trees are used over hash maps for indexing (range scans)
- How buffer pools hide disk latency behind an in-memory cache
- Why write-ahead logging is necessary for crash recovery
- How a recursive descent parser works without any libraries
- How SQL execution plans are built and optimized
- Why tombstone deletion is preferred over immediate compaction

---

## 🙏 References

- **CMU 15-445** — Andy Pavlo's Database Systems course (YouTube + assignments)
- **Database Internals** — Alex Petrov (storage engine architecture)
- **Architecture of a Database System** — Hellerstein, Stonebraker, Hamilton

---



<div align="center">
  <strong>Built from scratch — no database libraries used</strong><br>
  <sub>Every byte on disk, every token in the parser, every node in the B+ tree — written by hand.</sub>
</div>