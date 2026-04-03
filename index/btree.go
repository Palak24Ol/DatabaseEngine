package index

import "fmt"

// order = max children per internal node
// each node holds at most (order-1) keys
const order = 4

// BPlusTree — the heart of every real database index
type BPlusTree struct {
	root *bnode
}

type bnode struct {
	isLeaf   bool
	keys     []int
	vals     []uint32 // leaf only: stores values (e.g. RID offsets)
	children []*bnode // internal only: child pointers
	next     *bnode   // leaf only: linked list to next leaf
}

// NewBPlusTree creates an empty B+ tree
func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		root: &bnode{isLeaf: true},
	}
}

// Search finds the value for a key — O(log n)
func (t *BPlusTree) Search(key int) (uint32, bool) {
	leaf := t.findLeaf(key)
	for i, k := range leaf.keys {
		if k == key {
			return leaf.vals[i], true
		}
	}
	return 0, false
}

// Insert adds a key-value pair into the tree
func (t *BPlusTree) Insert(key int, val uint32) {
	leaf := t.findLeaf(key)
	insertIntoLeaf(leaf, key, val)

	// if leaf overflows, split it
	if len(leaf.keys) >= order {
		t.splitLeaf(leaf)
	}
}

// Delete removes a key from the tree
// (simplified: no rebalancing — good enough for Phase 2)
func (t *BPlusTree) Delete(key int) bool {
	leaf := t.findLeaf(key)
	for i, k := range leaf.keys {
		if k == key {
			leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
			leaf.vals = append(leaf.vals[:i], leaf.vals[i+1:]...)
			return true
		}
	}
	return false
}

// RangeScan returns all values with keys in [startKey, endKey]
// This is why B+ trees are amazing — leaf linked list makes range scans fast
func (t *BPlusTree) RangeScan(startKey, endKey int) []uint32 {
	leaf := t.findLeaf(startKey)
	var results []uint32

	for leaf != nil {
		for i, k := range leaf.keys {
			if k > endKey {
				return results
			}
			if k >= startKey {
				results = append(results, leaf.vals[i])
			}
		}
		leaf = leaf.next // jump to next leaf — O(1)!
	}

	return results
}

// Print displays the tree structure for debugging
func (t *BPlusTree) Print() {
	fmt.Println("=== B+ Tree ===")
	printNode(t.root, 0)
	fmt.Println("===============")
}

// ── internal helpers ──────────────────────────────────────────

func (t *BPlusTree) findLeaf(key int) *bnode {
	cur := t.root
	for !cur.isLeaf {
		i := 0
		for i < len(cur.keys) && key >= cur.keys[i] {
			i++
		}
		cur = cur.children[i]
	}
	return cur
}

func insertIntoLeaf(leaf *bnode, key int, val uint32) {
	i := 0
	for i < len(leaf.keys) && leaf.keys[i] < key {
		i++
	}
	leaf.keys = append(leaf.keys, 0)
	leaf.vals = append(leaf.vals, 0)
	copy(leaf.keys[i+1:], leaf.keys[i:])
	copy(leaf.vals[i+1:], leaf.vals[i:])
	leaf.keys[i] = key
	leaf.vals[i] = val
}

func (t *BPlusTree) splitLeaf(leaf *bnode) {
	mid := len(leaf.keys) / 2

	newLeaf := &bnode{
		isLeaf: true,
		keys:   append([]int{}, leaf.keys[mid:]...),
		vals:   append([]uint32{}, leaf.vals[mid:]...),
		next:   leaf.next,
	}
	leaf.keys = leaf.keys[:mid]
	leaf.vals = leaf.vals[:mid]
	leaf.next = newLeaf

	t.insertIntoParent(leaf, newLeaf.keys[0], newLeaf)
}

func (t *BPlusTree) insertIntoParent(left *bnode, key int, right *bnode) {
	if left == t.root {
		// tree grows upward — create a new root
		t.root = &bnode{
			isLeaf:   false,
			keys:     []int{key},
			children: []*bnode{left, right},
		}
		return
	}

	parent := t.findParent(t.root, left)
	if parent == nil {
		return
	}

	// insert key into parent at correct position
	i := 0
	for i < len(parent.keys) && parent.keys[i] < key {
		i++
	}
	parent.keys = append(parent.keys, 0)
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = key

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = right

	// if parent overflows, split it too
	if len(parent.keys) >= order {
		t.splitInternal(parent)
	}
}

func (t *BPlusTree) splitInternal(n *bnode) {
	mid := len(n.keys) / 2
	midKey := n.keys[mid]

	newNode := &bnode{
		isLeaf:   false,
		keys:     append([]int{}, n.keys[mid+1:]...),
		children: append([]*bnode{}, n.children[mid+1:]...),
	}
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	t.insertIntoParent(n, midKey, newNode)
}

func (t *BPlusTree) findParent(cur *bnode, target *bnode) *bnode {
	if cur.isLeaf {
		return nil
	}
	for _, child := range cur.children {
		if child == target {
			return cur
		}
		if result := t.findParent(child, target); result != nil {
			return result
		}
	}
	return nil
}

func printNode(n *bnode, level int) {
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}
	if n.isLeaf {
		fmt.Printf("%s📄 Leaf: keys=%v vals=%v\n", indent, n.keys, n.vals)
	} else {
		fmt.Printf("%s🔵 Internal: keys=%v\n", indent, n.keys)
		for _, child := range n.children {
			printNode(child, level+1)
		}
	}
}