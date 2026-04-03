package storage

import (
	"encoding/binary"
	"fmt"
)

// RID uniquely identifies a record — page + offset + length
type RID struct {
	PageID uint32
	Offset uint32
	Length uint32
}

const (
	TUPLE_ALIVE   = byte(0x01)
	TUPLE_DELETED = byte(0x00)
)

// HeapFile manages an unordered collection of pages
type HeapFile struct {
	disk       *DiskManager
	lastPageID int32
}

// NewHeapFile creates a heap file backed by a disk manager
func NewHeapFile(disk *DiskManager) *HeapFile {
	lastPageID := int32(disk.GetNumPages()) - 1
	return &HeapFile{
		disk:       disk,
		lastPageID: lastPageID,
	}
}

// InsertTuple inserts data — format: [1 flag][4 length][data]
func (hf *HeapFile) InsertTuple(data []byte) (RID, error) {
	// build record: [1 byte flag][4 bytes length][data]
	record := make([]byte, 1+4+len(data))
	record[0] = TUPLE_ALIVE
	binary.LittleEndian.PutUint32(record[1:5], uint32(len(data)))
	copy(record[5:], data)

	// try inserting into last page first
	if hf.lastPageID >= 0 {
		page, err := hf.disk.ReadPage(uint32(hf.lastPageID))
		if err == nil {
			offset := page.GetFreeSpaceOffset()
			_, ok := page.InsertTuple(record)
			if ok {
				hf.disk.WritePage(page)
				return RID{PageID: uint32(hf.lastPageID), Offset: offset, Length: uint32(len(data))}, nil
			}
		}
	}

	// allocate new page
	page := hf.disk.AllocatePage()
	offset := page.GetFreeSpaceOffset()
	_, ok := page.InsertTuple(record)
	if !ok {
		return RID{}, fmt.Errorf("tuple too large to fit in a single page")
	}
	hf.disk.WritePage(page)
	hf.lastPageID = int32(page.ID)
	return RID{PageID: page.ID, Offset: offset, Length: uint32(len(data))}, nil
}

// GetTuple retrieves a tuple by its RID
func (hf *HeapFile) GetTuple(rid RID) ([]byte, error) {
	page, err := hf.disk.ReadPage(rid.PageID)
	if err != nil {
		return nil, fmt.Errorf("heap: failed to read page: %w", err)
	}
	// skip flag(1) + length(4)
	record := page.GetTuple(rid.Offset, rid.Length+5)
	return record[5:], nil
}

// DeleteTuple marks a tuple as deleted using tombstone flag
func (hf *HeapFile) DeleteTuple(rid RID) error {
	page, err := hf.disk.ReadPage(rid.PageID)
	if err != nil {
		return err
	}
	// set tombstone flag to DELETED
	page.Data[rid.Offset] = TUPLE_DELETED
	return hf.disk.WritePage(page)
}

// Scan iterates over all ALIVE tuples
func (hf *HeapFile) Scan(callback func(RID, []byte)) error {
	numPages := hf.disk.GetNumPages()
	for pageID := uint32(0); pageID < numPages; pageID++ {
		page, err := hf.disk.ReadPage(pageID)
		if err != nil {
			return err
		}
		offset := uint32(HeaderSize)
		endOffset := page.GetFreeSpaceOffset()

		for offset < endOffset {
			if offset+5 > endOffset {
				break
			}
			flag := page.Data[offset]
			length := binary.LittleEndian.Uint32(page.Data[offset+1 : offset+5])
			if length == 0 {
				break
			}
			if flag == TUPLE_ALIVE {
				data := make([]byte, length)
				copy(data, page.Data[offset+5:offset+5+length])
				rid := RID{PageID: pageID, Offset: offset, Length: length}
				callback(rid, data)
			}
			offset += 1 + 4 + length
		}
	}
	return nil
}