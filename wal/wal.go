package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// LogType identifies what kind of WAL record this is
type LogType uint8

const (
	LOG_BEGIN  LogType = iota // transaction started
	LOG_INSERT                // row inserted
	LOG_DELETE                // row deleted
	LOG_COMMIT                // transaction committed
	LOG_ABORT                 // transaction aborted
)

// LogRecord is a single entry in the WAL
type LogRecord struct {
	LSN       uint64  // Log Sequence Number — unique ID
	TxID      uint64  // which transaction this belongs to
	Type      LogType // what kind of operation
	TableName string  // which table
	Data      []byte  // the actual row data (for INSERT)
	Timestamp int64   // unix timestamp
}

// WAL is the Write-Ahead Log — append-only file on disk
type WAL struct {
	file    *os.File
	nextLSN uint64
	nextTxID uint64
}

// NewWAL opens or creates a WAL file
func NewWAL(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	// figure out next LSN from file size
	info, _ := file.Stat()
	nextLSN := uint64(info.Size()/64) + 1

	return &WAL{
		file:     file,
		nextLSN:  nextLSN,
		nextTxID: 1,
	}, nil
}

// Begin starts a new transaction and returns its ID
func (w *WAL) Begin() uint64 {
	txID := w.nextTxID
	w.nextTxID++
	w.writeRecord(&LogRecord{
		LSN:       w.nextLSN,
		TxID:      txID,
		Type:      LOG_BEGIN,
		Timestamp: time.Now().Unix(),
	})
	return txID
}

// LogInsert records an INSERT operation
func (w *WAL) LogInsert(txID uint64, tableName string, data []byte) {
	w.writeRecord(&LogRecord{
		LSN:       w.nextLSN,
		TxID:      txID,
		Type:      LOG_INSERT,
		TableName: tableName,
		Data:      data,
		Timestamp: time.Now().Unix(),
	})
}

// LogDelete records a DELETE operation
func (w *WAL) LogDelete(txID uint64, tableName string, data []byte) {
	w.writeRecord(&LogRecord{
		LSN:       w.nextLSN,
		TxID:      txID,
		Type:      LOG_DELETE,
		TableName: tableName,
		Data:      data,
		Timestamp: time.Now().Unix(),
	})
}

// Commit marks a transaction as committed
func (w *WAL) Commit(txID uint64) {
	w.writeRecord(&LogRecord{
		LSN:       w.nextLSN,
		TxID:      txID,
		Type:      LOG_COMMIT,
		Timestamp: time.Now().Unix(),
	})
	w.file.Sync() // fsync — guarantee durability
}

// Abort marks a transaction as aborted
func (w *WAL) Abort(txID uint64) {
	w.writeRecord(&LogRecord{
		LSN:       w.nextLSN,
		TxID:      txID,
		Type:      LOG_ABORT,
		Timestamp: time.Now().Unix(),
	})
}

// ReadAll reads all log records — used for crash recovery
func (w *WAL) ReadAll() ([]*LogRecord, error) {
	// reopen from beginning
	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []*LogRecord
	for {
		rec, err := readRecord(file)
		if err != nil {
			break
		}
		records = append(records, rec)
	}
	return records, nil
}

// PrintLog pretty prints the WAL for debugging
func (w *WAL) PrintLog() {
	records, err := w.ReadAll()
	if err != nil || len(records) == 0 {
		fmt.Println("   (empty log)")
		return
	}

	typeNames := map[LogType]string{
		LOG_BEGIN:  "BEGIN ",
		LOG_INSERT: "INSERT",
		LOG_DELETE: "DELETE",
		LOG_COMMIT: "COMMIT",
		LOG_ABORT:  "ABORT ",
	}

	for _, r := range records {
		typeName := typeNames[r.Type]
		if r.TableName != "" {
			fmt.Printf("   LSN=%-4d TxID=%-3d [%s] table='%s'\n",
				r.LSN, r.TxID, typeName, r.TableName)
		} else {
			fmt.Printf("   LSN=%-4d TxID=%-3d [%s]\n",
				r.LSN, r.TxID, typeName)
		}
	}
}

func (w *WAL) Close() error {
	return w.file.Close()
}

// ── Internal helpers ──────────────────────────────────────────

// writeRecord serializes a log record to disk
// Format: [8 LSN][8 TxID][1 Type][8 Timestamp][4 tableLen][table][4 dataLen][data]
func (w *WAL) writeRecord(rec *LogRecord) {
	rec.LSN = w.nextLSN
	w.nextLSN++

	tableBytes := []byte(rec.TableName)

	buf := make([]byte, 8+8+1+8+4+len(tableBytes)+4+len(rec.Data))
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], rec.LSN)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], rec.TxID)
	offset += 8
	buf[offset] = byte(rec.Type)
	offset++
	binary.LittleEndian.PutUint64(buf[offset:], uint64(rec.Timestamp))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(tableBytes)))
	offset += 4
	copy(buf[offset:], tableBytes)
	offset += len(tableBytes)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(rec.Data)))
	offset += 4
	copy(buf[offset:], rec.Data)

	w.file.Write(buf)
}

func readRecord(file *os.File) (*LogRecord, error) {
	// read fixed header: LSN(8) + TxID(8) + Type(1) + Timestamp(8) = 25 bytes
	header := make([]byte, 25)
	if _, err := file.Read(header); err != nil {
		return nil, err
	}

	rec := &LogRecord{
		LSN:       binary.LittleEndian.Uint64(header[0:8]),
		TxID:      binary.LittleEndian.Uint64(header[8:16]),
		Type:      LogType(header[16]),
		Timestamp: int64(binary.LittleEndian.Uint64(header[17:25])),
	}

	// read table name
	tableLen := make([]byte, 4)
	if _, err := file.Read(tableLen); err != nil {
		return nil, err
	}
	tLen := binary.LittleEndian.Uint32(tableLen)
	if tLen > 0 {
		tableBytes := make([]byte, tLen)
		file.Read(tableBytes)
		rec.TableName = string(tableBytes)
	}

	// read data
	dataLen := make([]byte, 4)
	if _, err := file.Read(dataLen); err != nil {
		return nil, err
	}
	dLen := binary.LittleEndian.Uint32(dataLen)
	if dLen > 0 {
		rec.Data = make([]byte, dLen)
		file.Read(rec.Data)
	}

	return rec, nil
}