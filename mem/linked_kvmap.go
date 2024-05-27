package mem

import (
	"github.com/spirit-labs/tektite/common"
)

// Node represents a node in the doubly linked list
type Node struct {
	value common.KV
	prev  *Node
	next  *Node
}

// LinkedKVMap represents the linked hash map
// It's a version aware map of KVs. It only maintains the most recently put KV, ignoring version.
type LinkedKVMap struct {
	m    map[string]*Node
	head *Node
	tail *Node
}

const initialMapSize = 1000

// NewLinkedKVMap creates a new linked hash map
func NewLinkedKVMap() *LinkedKVMap {
	return &LinkedKVMap{
		m: make(map[string]*Node, initialMapSize),
	}
}

// Put adds a key-value pair to the linked hash map
func (l *LinkedKVMap) Put(kv common.KV) {
	keyNoVersion := kv.Key[:len(kv.Key)-8]
	sKey := common.ByteSliceToStringZeroCopy(keyNoVersion)

	if node, ok := l.m[sKey]; ok {
		// Update the existing node's value
		node.value = kv
		return
	}

	// Create a new node
	newNode := &Node{
		value: kv,
		prev:  l.tail,
	}

	// Update the tail
	if l.tail != nil {
		l.tail.next = newNode
	}
	l.tail = newNode

	// Update the head if it's the first node
	if l.head == nil {
		l.head = newNode
	}

	// Update the map
	l.m[sKey] = newNode
}

// Get retrieves the value associated with a key from the linked hash map
// Note, the key to get does not have the version in it.
func (l *LinkedKVMap) Get(key []byte) ([]byte, bool) {
	sKey := common.ByteSliceToStringZeroCopy(key)
	if node, ok := l.m[sKey]; ok {
		return node.value.Value, true
	}
	return nil, false
}

// Delete removes a key-value pair from the linked hash map
func (l *LinkedKVMap) Delete(key []byte) {
	sKey := common.ByteSliceToStringZeroCopy(key)
	if node, ok := l.m[sKey]; ok {
		// Update the next node's prev pointer
		if node.next != nil {
			node.next.prev = node.prev
		}

		// Update the prev node's next pointer
		if node.prev != nil {
			node.prev.next = node.next
		}

		// Update head and tail pointers if needed
		if node == l.head {
			l.head = node.next
		}
		if node == l.tail {
			l.tail = node.prev
		}

		// Remove the key from the map
		delete(l.m, sKey)
	}
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, Range stops the iteration.
func (l *LinkedKVMap) Range(f func(key []byte, value []byte) bool) {
	current := l.head
	for current != nil {
		if !f(current.value.Key, current.value.Value) {
			break
		}
		current = current.next
	}
}

func (l *LinkedKVMap) Len() int {
	return len(l.m)
}
