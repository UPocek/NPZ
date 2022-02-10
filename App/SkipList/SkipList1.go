package SkipList

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	tombstone bool
	timestamp int64
	next      []*SkipListNode
}

func CreateSkipList(maxHeight int, height int, size int) *SkipList {
	now := time.Now()
	timestamp := now.Unix()
	node := createNode("", []byte("none"), maxHeight+1, timestamp)
	return &SkipList{
		maxHeight: maxHeight,
		height:    height,
		size:      size,
		head:      node,
	}
}
func createNode(key string, val []byte, height int, timestamp int64) *SkipListNode {
	return &SkipListNode{
		key:       key,
		value:     val,
		timestamp: timestamp,
		tombstone: false,
		next:      make([]*SkipListNode, height),
	}
}
func (node *SkipListNode) GetValue() []byte {
	return node.value
}

func (node *SkipListNode) GetKey() string {
	return node.key
}

func (node *SkipListNode) GetTimestamp() int64 {
	return node.timestamp
}

func (node *SkipListNode) GetTombstone() bool {
	return node.tombstone
}

func (s *SkipList) roll() int {
	level := 0 // alwasy start from level 0
	// We roll until we don't get 1 from rand function and we did not
	// outgrow maxHeight. BUT rand can give us 0, and if that is the case
	// than we will just increase level, and wait for 1 from rand!
	for rand.Int31n(2) == 1 {
		level++
		if level == s.maxHeight {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			s.height = level
			return level
		}

	}
	return level
}
func (sl *SkipList) addFirstNode(node *SkipListNode, levels int) {
	for i := 0; i <= levels; i++ {
		sl.head.next[i] = node
	}
}
func (sl *SkipList) addOnLevels(node *SkipListNode, levels int) {
	curr := sl.head
	for i := levels; i >= 0; i-- {
		if curr.next[i] == nil {
			//ako je kraj liste na tom nivou
			curr.next[i] = node
		} else {
			for curr.next[i].key < node.key {
				curr = curr.next[i]
				if curr.next[i] == nil {
					//ako je kraj liste na tom nivou
					break
				}
			}
			node.next[i] = curr.next[i]
			curr.next[i] = node
		}
		curr = sl.head
	}
}
func (sl *SkipList) AddElement(key string, value []byte) (error, bool) {
	node := sl.FindElement(key)
	if node == nil {
		levels := sl.roll()
		if levels > sl.height {
			sl.height = levels
		}
		now := time.Now()
		timestamp := now.Unix()
		node := createNode(key, value, levels+1, timestamp)
		curr := sl.head
		if curr.next[0] == nil {
			sl.addFirstNode(node, levels)
		} else {
			sl.addOnLevels(node, levels)
		}
		sl.size += 1
		return nil, true
	} else {
		now := time.Now()
		timestamp := now.Unix()
		node.value = value
		node.tombstone = false
		node.timestamp = timestamp
		return nil, false
	}
}

func (sl *SkipList) UpdateTimestamp(key string, value []byte, ts int64, whatToDo byte) (error, bool) {
	if whatToDo == 0 {
		node := sl.FindElement(key)
		if node == nil {
			levels := sl.roll()
			if levels > sl.height {
				sl.height = levels
			}
			node := createNode(key, value, levels+1, ts)
			curr := sl.head
			if curr.next[0] == nil {
				sl.addFirstNode(node, levels)
			} else {
				sl.addOnLevels(node, levels)
			}
			sl.size += 1
			return nil, true
		} else {
			node.value = value
			node.tombstone = false
			node.timestamp = ts
			return nil, false
		}
	} else {
		node := sl.FindElement(key)
		if node == nil {
			return fmt.Errorf("Potrebna provera"), false
		}
		if node.tombstone == false {
			node.tombstone = true
			node.timestamp = ts
			return nil, false
		}
		return nil, false
	}
}

func (sl *SkipList) FindElement(key string) *SkipListNode {
	level := sl.height
	curr_key := sl.head
	for curr_key.key != key {
		if curr_key.next[level] == nil || curr_key.next[level].key > key {
			if level == 0 {
				//log.Fatal("No match!")
				return nil
			} else {
				level -= 1
			}
		} else {
			curr_key = curr_key.next[level]
		}

	}
	return curr_key
}

func (sl *SkipList) RemoveElement(key string) uint8 {
	node := sl.FindElement(key)
	if node == nil {
		return 1
	}
	if node.tombstone == false {
		node.tombstone = true
		now := time.Now()
		timestamp := now.Unix()
		node.timestamp = timestamp
		return 0
	}
	return 2
}

func (skip *SkipList) LastLevel() []*SkipListNode {
	curr := skip.head
	res := make([]*SkipListNode, 0)
	curr = curr.next[0]
	res = append(res, curr)
	for i := 1; i < skip.size; i++ {
		curr = curr.next[0]
		res = append(res, curr)
	}
	return res
}

func (sl *SkipList) AddDeletedElement(key string, value []byte, ts int64) error {
	node := sl.FindElement(key)
	if node != nil {
		levels := sl.roll()
		node := createNode(key, value, levels+1, ts)
		node.tombstone = true
		curr := sl.head
		if curr.next[0] == nil {
			sl.addFirstNode(node, levels)
		} else {
			sl.addOnLevels(node, levels)
		}

		sl.size += 1
		return nil
	} else {
		node.value = value
		node.tombstone = true
		node.timestamp = ts
		return nil
	}
}

func main() {
	sl := CreateSkipList(5, 0, 1)
	sl.AddElement("key1", []byte("mama"))
	node := sl.FindElement("key1")
	fmt.Printf(string(node.value))
	sl.RemoveElement("key1")
	sl.RemoveElement("key2")
	fmt.Printf(string(node.value))
}
