package storage

import (
	"fmt"
	"os"
)

// DiskManager handles reading and writing pages to disk
type DiskManager struct {
	file     *os.File
	numPages uint32
}

// NewDiskManager opens (or creates) a .db file
func NewDiskManager(filepath string) (*DiskManager, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open db file: %w", err)
	}

	// figure out how many pages already exist
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	numPages := uint32(info.Size()) / PageSize

	return &DiskManager{
		file:     file,
		numPages: numPages,
	}, nil
}

// WritePage writes a page to disk at the correct offset
func (dm *DiskManager) WritePage(page *Page) error {
	offset := int64(page.ID) * PageSize

	_, err := dm.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}

	// update page count if this is a new page
	if page.ID >= dm.numPages {
		dm.numPages = page.ID + 1
	}

	return nil
}

// ReadPage reads a page from disk by its ID
func (dm *DiskManager) ReadPage(pageID uint32) (*Page, error) {
	if pageID >= dm.numPages {
		return nil, fmt.Errorf("page %d does not exist", pageID)
	}

	page := &Page{ID: pageID}
	offset := int64(pageID) * PageSize

	_, err := dm.file.ReadAt(page.Data[:], offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	return page, nil
}

// AllocatePage creates a new blank page and returns it
func (dm *DiskManager) AllocatePage() *Page {
	page := NewPage(dm.numPages)
	dm.numPages++
	return page
}

// GetNumPages returns total number of pages in the db file
func (dm *DiskManager) GetNumPages() uint32 {
	return dm.numPages
}

// Close closes the db file
func (dm *DiskManager) Close() error {
	return dm.file.Close()
}