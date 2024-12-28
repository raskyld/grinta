package grinta

// Forked from https://github.com/armon/go-radix
// Just made generic to benefit from future compile-time
// optimisation + migrate to Go 1.23 iterators.

import (
	"iter"
	"sort"
	"strings"
)

// leafNode is used to represent a value
type leafNode[T any] struct {
	key string
	val T
}

// edge is used to represent an edge node
type edge[T any] struct {
	label byte
	node  *node[T]
}

type node[T any] struct {
	// leaf is used to store possible leaf
	leaf *leafNode[T]

	// prefix is the common prefix we ignore
	prefix string

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse
	edges edges[T]
}

func (n *node[T]) isLeaf() bool {
	return n.leaf != nil
}

func (n *node[T]) addEdge(e edge[T]) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})

	n.edges = append(n.edges, edge[T]{})
	copy(n.edges[idx+1:], n.edges[idx:])
	n.edges[idx] = e
}

func (n *node[T]) updateEdge(label byte, node *node[T]) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		n.edges[idx].node = node
		return
	}
	panic("replacing missing edge")
}

func (n *node[T]) getEdge(label byte) *node[T] {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return n.edges[idx].node
	}
	return nil
}

func (n *node[T]) delEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge[T]{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

type edges[T any] []edge[T]

func (e edges[T]) Len() int {
	return len(e)
}

func (e edges[T]) Less(i, j int) bool {
	return e[i].label < e[j].label
}

func (e edges[T]) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e edges[T]) Sort() {
	sort.Sort(e)
}

// Tree implements a radix tree. This can be treated as a
// Dictionary abstract data type. The main advantage over
// a standard hash map is prefix-based lookups and
// ordered iteration,
type Tree[T any] struct {
	root *node[T]
	size int
}

// New returns an empty Tree
func NewTree[T any]() *Tree[T] {
	return NewTreeFromMap[T](nil)
}

// NewFromMap returns a new tree containing the keys
// from an existing map
func NewTreeFromMap[T any](m map[string]T) *Tree[T] {
	t := &Tree[T]{root: &node[T]{}}
	for k, v := range m {
		t.Insert(k, v)
	}
	return t
}

// Len is used to return the number of elements in the tree
func (t *Tree[T]) Len() int {
	return t.size
}

// longestPrefix finds the length of the shared prefix
// of two strings
func longestPrefix(k1, k2 string) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

// Insert is used to add a newentry or update
// an existing entry. Returns true if an existing record is updated.
func (t *Tree[T]) Insert(s string, v T) (old T, updated bool) {
	var parent *node[T]
	n := t.root
	search := s
	for {
		// Handle key exhaution
		if len(search) == 0 {
			if n.isLeaf() {
				old = n.leaf.val
				n.leaf.val = v
				return old, true
			}

			n.leaf = &leafNode[T]{
				key: s,
				val: v,
			}
			t.size++
			return old, false
		}

		// Look for the edge
		parent = n
		n = n.getEdge(search[0])

		// No edge, create one
		if n == nil {
			e := edge[T]{
				label: search[0],
				node: &node[T]{
					leaf: &leafNode[T]{
						key: s,
						val: v,
					},
					prefix: search,
				},
			}
			parent.addEdge(e)
			t.size++
			return old, false
		}

		// Determine longest prefix of the search key on match
		commonPrefix := longestPrefix(search, n.prefix)
		if commonPrefix == len(n.prefix) {
			search = search[commonPrefix:]
			continue
		}

		// Split the node
		t.size++
		child := &node[T]{
			prefix: search[:commonPrefix],
		}
		parent.updateEdge(search[0], child)

		// Restore the existing node
		child.addEdge(edge[T]{
			label: n.prefix[commonPrefix],
			node:  n,
		})
		n.prefix = n.prefix[commonPrefix:]

		// Create a new leaf node
		leaf := &leafNode[T]{
			key: s,
			val: v,
		}

		// If the new key is a subset, add to this node
		search = search[commonPrefix:]
		if len(search) == 0 {
			child.leaf = leaf
			return old, false
		}

		// Create a new edge for the node
		child.addEdge(edge[T]{
			label: search[0],
			node: &node[T]{
				leaf:   leaf,
				prefix: search,
			},
		})
		return old, false
	}
}

// Delete is used to delete a key, returning the previous
// value and if it was deleted
func (t *Tree[T]) Delete(s string) (removed T, hasRemoved bool) {
	var parent *node[T]
	var label byte
	n := t.root
	search := s
	for {
		// Check for key exhaution
		if len(search) == 0 {
			if !n.isLeaf() {
				break
			}
			goto DELETE
		}

		// Look for an edge
		parent = n
		label = search[0]
		n = n.getEdge(label)
		if n == nil {
			break
		}

		// Consume the search prefix
		if strings.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return

DELETE:
	// Delete the leaf
	leaf := n.leaf
	n.leaf = nil
	t.size--

	// Check if we should delete this node from the parent
	if parent != nil && len(n.edges) == 0 {
		parent.delEdge(label)
	}

	// Check if we should merge this node
	if n != t.root && len(n.edges) == 1 {
		n.mergeChild()
	}

	// Check if we should merge the parent's other child
	if parent != nil && parent != t.root && len(parent.edges) == 1 && !parent.isLeaf() {
		parent.mergeChild()
	}

	return leaf.val, true
}

// DeletePrefix is used to delete the subtree under a prefix
// Returns how many nodes were deleted
// Use this to delete large subtrees efficiently
func (t *Tree[T]) DeletePrefix(s string) int {
	return t.deletePrefix(nil, t.root, s)
}

// delete does a recursive deletion
func (t *Tree[T]) deletePrefix(parent, n *node[T], prefix string) int {
	// Check for key exhaustion
	if len(prefix) == 0 {
		// Remove the leaf node
		subTreeSize := 0
		//recursively walk from all edges of the node to be deleted
		for range recursiveWalk(n) {
			subTreeSize++
		}
		if n.isLeaf() {
			n.leaf = nil
		}
		n.edges = nil // deletes the entire subtree

		// Check if we should merge the parent's other child
		if parent != nil && parent != t.root && len(parent.edges) == 1 && !parent.isLeaf() {
			parent.mergeChild()
		}
		t.size -= subTreeSize
		return subTreeSize
	}

	// Look for an edge
	label := prefix[0]
	child := n.getEdge(label)
	if child == nil || (!strings.HasPrefix(child.prefix, prefix) && !strings.HasPrefix(prefix, child.prefix)) {
		return 0
	}

	// Consume the search prefix
	if len(child.prefix) > len(prefix) {
		prefix = prefix[len(prefix):]
	} else {
		prefix = prefix[len(child.prefix):]
	}
	return t.deletePrefix(n, child, prefix)
}

func (n *node[T]) mergeChild() {
	e := n.edges[0]
	child := e.node
	n.prefix = n.prefix + child.prefix
	n.leaf = child.leaf
	n.edges = child.edges
}

// Get is used to lookup a specific key, returning
// the value and if it was found
func (t *Tree[T]) Get(s string) (val T, found bool) {
	n := t.root
	search := s
	for {
		// Check for key exhaution
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.val, true
			}
			break
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if strings.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return
}

// LongestPrefix is like Get, but instead of an
// exact match, it will return the longest prefix match.
func (t *Tree[T]) LongestPrefix(s string) (pref string, val T, found bool) {
	var last *leafNode[T]
	n := t.root
	search := s
	for {
		// Look for a leaf node
		if n.isLeaf() {
			last = n.leaf
		}

		// Check for key exhaution
		if len(search) == 0 {
			break
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if strings.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return last.key, last.val, true
	}
	return
}

// Minimum is used to return the minimum value in the tree
func (t *Tree[T]) Minimum() (key string, val T, found bool) {
	n := t.root
	for {
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		}
		if len(n.edges) > 0 {
			n = n.edges[0].node
		} else {
			break
		}
	}
	return
}

// Maximum is used to return the maximum value in the tree
func (t *Tree[T]) Maximum() (key string, val T, found bool) {
	n := t.root
	for {
		if num := len(n.edges); num > 0 {
			n = n.edges[num-1].node
			continue
		}
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		}
		break
	}
	return
}

// Walk is used to walk the tree
func (t *Tree[T]) Walk() iter.Seq2[string, T] {
	return recursiveWalk(t.root)
}

// WalkPrefix is used to walk the tree under a prefix
func (t *Tree[T]) WalkPrefix(prefix string) iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		n := t.root
		search := prefix
		for {
			// Check for key exhaustion
			if len(search) == 0 {
				recursiveWalk(n)(yield)
				return
			}

			// Look for an edge
			n = n.getEdge(search[0])
			if n == nil {
				return
			}

			// Consume the search prefix
			if strings.HasPrefix(search, n.prefix) {
				search = search[len(n.prefix):]
				continue
			}
			if strings.HasPrefix(n.prefix, search) {
				// Child may be under our search prefix
				recursiveWalk(n)(yield)
			}
			return
		}
	}
}

// WalkPath is used to walk the tree, but only visiting nodes
// from the root down to a given leaf. Where WalkPrefix walks
// all the entries *under* the given prefix, this walks the
// entries *above* the given prefix.
func (t *Tree[T]) WalkPath(path string) iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		n := t.root
		search := path
		for {
			// Visit the leaf values if any
			if n.leaf != nil && !yield(n.leaf.key, n.leaf.val) {
				return
			}

			// Check for key exhaution
			if len(search) == 0 {
				return
			}

			// Look for an edge
			n = n.getEdge(search[0])
			if n == nil {
				return
			}

			// Consume the search prefix
			if strings.HasPrefix(search, n.prefix) {
				search = search[len(n.prefix):]
			} else {
				break
			}
		}
	}
}

func recursiveWalk[T any](n *node[T]) iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		recursiveWalkInner(n, yield)
	}
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalkInner[T any](n *node[T], yield func(string, T) bool) bool {
	// Visit the leaf values if any
	if n.leaf != nil && !yield(n.leaf.key, n.leaf.val) {
		return true
	}

	// Recurse on the children
	i := 0
	k := len(n.edges) // keeps track of number of edges in previous iteration
	for i < k {
		e := n.edges[i]
		if recursiveWalkInner(e.node, yield) {
			return true
		}
		// It is a possibility that the WalkFn modified the node we are
		// iterating on. If there are no more edges, mergeChild happened,
		// so the last edge became the current node n, on which we'll
		// iterate one last time.
		if len(n.edges) == 0 {
			return recursiveWalkInner(n, yield)
		}
		// If there are now less edges than in the previous iteration,
		// then do not increment the loop index, since the current index
		// points to a new edge. Otherwise, get to the next index.
		if len(n.edges) >= k {
			i++
		}
		k = len(n.edges)
	}
	return false
}

// ToMap is used to walk the tree and convert it into a map
func (t *Tree[T]) ToMap() map[string]T {
	out := make(map[string]T, t.size)
	for k, v := range t.Walk() {
		out[k] = v
	}
	return out
}
