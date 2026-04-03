package buffer

import (
	"container/list"
	"fmt"
	"dbengine/storage"
)

const PoolSize = 10 // number of pages we keep in memory

// frame holds a page + metadata
type frame struct {
	page  *storage.Page
	dirty bool // has this page been modified?
}

// BufferPool caches pages in memory using LRU eviction
type BufferPool struct {
	frames   map[uint32]*list.Element // pageID -> LRU element
	lruList  *list.List               // front = most recent, back = least recent
	disk     *storage.DiskManager
	capacity int
}

// NewBufferPool creates a buffer pool backed by a disk manager
func NewBufferPool(disk *storage.DiskManager) *BufferPool {
	return &BufferPool{
		frames:   make(map[uint32]*list.Element),
		lruList:  list.New(),
		disk:     disk,
		capacity: PoolSize,
	}
}

// FetchPage gets a page — from cache if possible, else from disk
func (bp *BufferPool) FetchPage(pageID uint32) (*storage.Page, error) {
	// cache hit
	if elem, ok := bp.frames[pageID]; ok {
		bp.lruList.MoveToFront(elem)
		return elem.Value.(*frame).page, nil
	}

	// cache miss — read from disk
	page, err := bp.disk.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("buffer pool: %w", err)
	}

	// evict if full
	if bp.lruList.Len() >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	// add to cache
	f := &frame{page: page, dirty: false}
	elem := bp.lruList.PushFront(f)
	bp.frames[pageID] = elem

	return page, nil
}

// NewPage allocates a brand new page and puts it in the buffer pool
func (bp *BufferPool) NewPage() (*storage.Page, error) {
	// evict if full
	if bp.lruList.Len() >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	page := bp.disk.AllocatePage()

	f := &frame{page: page, dirty: true}
	elem := bp.lruList.PushFront(f)
	bp.frames[page.ID] = elem

	return page, nil
}

// MarkDirty marks a page as modified so it gets written back to disk
func (bp *BufferPool) MarkDirty(pageID uint32) {
	if elem, ok := bp.frames[pageID]; ok {
		elem.Value.(*frame).dirty = true
	}
}

// FlushPage writes a specific page to disk
func (bp *BufferPool) FlushPage(pageID uint32) error {
	elem, ok := bp.frames[pageID]
	if !ok {
		return nil
	}

	f := elem.Value.(*frame)
	if f.dirty {
		if err := bp.disk.WritePage(f.page); err != nil {
			return err
		}
		f.dirty = false
	}

	return nil
}

// FlushAll writes all dirty pages to disk
func (bp *BufferPool) FlushAll() error {
	for pageID := range bp.frames {
		if err := bp.FlushPage(pageID); err != nil {
			return err
		}
	}
	return nil
}

// evict removes the least recently used page, flushing if dirty
func (bp *BufferPool) evict() error {
	back := bp.lruList.Back()
	if back == nil {
		return fmt.Errorf("buffer pool is empty, cannot evict")
	}

	f := back.Value.(*frame)

	// flush to disk if dirty
	if f.dirty {
		if err := bp.disk.WritePage(f.page); err != nil {
			return fmt.Errorf("failed to evict page %d: %w", f.page.ID, err)
		}
	}

	// remove from cache
	delete(bp.frames, f.page.ID)
	bp.lruList.Remove(back)

	return nil
}