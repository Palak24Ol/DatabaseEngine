package storage

import "encoding/binary"

const (
	PageSize    = 4096 // 4KB per page, same as real databases
	HeaderSize  = 8    // 4 bytes for num slots, 4 bytes for free space offset
)

// Page represents a fixed-size block of data on disk
// This is the fundamental unit — everything lives inside pages
type Page struct {
	ID   uint32
	Data [PageSize]byte // raw bytes, exactly what's on disk
}

// NewPage creates a blank page with a given ID
func NewPage(id uint32) *Page {
	p := &Page{ID: id}
	// write num_slots = 0 at byte 0
	binary.LittleEndian.PutUint32(p.Data[0:4], 0)
	// write free_space_offset = HeaderSize at byte 4
	binary.LittleEndian.PutUint32(p.Data[4:8], HeaderSize)
	return p
}

// GetNumSlots reads how many tuples are stored in this page
func (p *Page) GetNumSlots() uint32 {
	return binary.LittleEndian.Uint32(p.Data[0:4])
}

// GetFreeSpaceOffset reads where free space begins
func (p *Page) GetFreeSpaceOffset() uint32 {
	return binary.LittleEndian.Uint32(p.Data[4:8])
}

// InsertTuple writes raw bytes into the page, returns slot index
func (p *Page) InsertTuple(data []byte) (uint32, bool) {
	offset := p.GetFreeSpaceOffset()
	numSlots := p.GetNumSlots()

	// check if enough space
	if int(offset)+len(data) > PageSize {
		return 0, false // page is full
	}

	// write the data at the current free offset
	copy(p.Data[offset:], data)

	// update free space offset
	binary.LittleEndian.PutUint32(p.Data[4:8], offset+uint32(len(data)))

	// update num slots
	binary.LittleEndian.PutUint32(p.Data[0:4], numSlots+1)

	return numSlots, true // return slot index
}

// GetTuple reads a tuple by its offset and length
func (p *Page) GetTuple(offset uint32, length uint32) []byte {
	return p.Data[offset : offset+length]
}