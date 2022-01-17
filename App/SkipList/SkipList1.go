package SkipList

import (
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
	key       float64
	value     []byte
	tombstone bool
	timestamp int64
	next      []*SkipListNode
}
func CreateSkipList(maxHeight int,height int, size int)*SkipList {
	node:= createNode(0, []byte("none"), maxHeight + 1)
	return &SkipList{
		maxHeight: maxHeight,
		height: height,
		size: size,
		head: node,
	}
}
func createNode(key float64, val []byte, height int) *SkipListNode {
	now := time.Now()
	timestamp := now.Unix()
	return &SkipListNode{
		key: key,
		value: val,
		timestamp: timestamp,
		tombstone: false,
		next: make([]*SkipListNode,height),
	}
}
func (node *SkipListNode) GetValue() []byte {
	return node.value
}

func (node *SkipListNode) GetKey() float64 {
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
	for ; rand.Int31n(2) == 1;{
		level++
		if level ==s.maxHeight {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			s.height = level
			return level
		}

	}
	return level
}
func (sl *SkipList) addFirstNode(node *SkipListNode,levels int)  {
	for i:=0;i<=levels;i++{
		sl.head.next[i]=node
	}
}
func (sl *SkipList) addOnLevels(node *SkipListNode,levels int){
	curr:=sl.head
	for i := levels; i >= 0;i-- {
		if curr.next[i]==nil{
			//ako je kraj liste na tom nivou
			curr.next[i] = node
		}else{
			for curr.next[i].key<node.key{
				curr=curr.next[i]
				if curr.next[i]==nil{
					//ako je kraj liste na tom nivou
					break
				}
			}
			node.next[i]=curr.next[i]
			curr.next[i]=node
		}
		curr=sl.head
	}
}
func (sl *SkipList) AddElement(key float64, value []byte) error {
	node := sl.FindElement(key)
	if node == nil{
		levels := sl.roll()
		if levels > sl.height {
			sl.height = levels
		}
		node := createNode(key,value,levels+1)
		curr := sl.head
		if curr.next[0] == nil{
			sl.addFirstNode(node, levels)
		} else {
			sl.addOnLevels(node, levels)

		}

		sl.size += 1
		return nil
	} else {
		now := time.Now()
		timestamp := now.Unix()
		node.value = value
		node.tombstone = false
		node.timestamp = timestamp
		return nil
	}

}

func (sl *SkipList) FindElement(key float64)*SkipListNode {
	level := sl.height
	curr_key := sl.head
	for curr_key.key != key{
		if curr_key.next[level] == nil || curr_key.next[level].key > key{
			if level == 0{
				//log.Fatal("No match!")
				return nil
			}else{
				level -= 1
			}
		}else{
			curr_key = curr_key.next[level]
		}

	}
	return curr_key
}

func (sl *SkipList) RemoveElement(key float64) uint8 {
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

func (skip *SkipList) LastLevel()[] *SkipListNode {
	curr := skip.head
	res:= make([]*SkipListNode, 0)
	curr=curr.next[0]
	res=append(res,curr)
	for i := 1; i <skip.size; i++{
		curr=curr.next[0]
		res=append(res,curr)
	}
	return res
}


func (sl *SkipList) AddDeletedElement(key float64, value []byte) error{
	node := sl.FindElement(key)
	if node != nil{
		levels := sl.roll()
		node := createNode(key,value,levels+1)
		node.tombstone = true
		curr := sl.head
		if curr.next[0] == nil{
			sl.addFirstNode(node, levels)
		} else {
			sl.addOnLevels(node, levels)
		}

		sl.size += 1
		return nil
	}else{
		now := time.Now()
		timestamp := now.Unix()
		node.value = value
		node.tombstone = true
		node.timestamp = timestamp
		return nil
	}
}

//func main() {
//	sl:= createSkipList(5,0,1)
//	sl.add(1,[]byte("mama"))
//	sl.add(6,[]byte("deda"))
//	sl.add(3,[]byte("seka"))
//	sl.add(13,[]byte("mika"))
//	sl.add(2,[]byte("joca"))
//	node:=sl.find(1)
//	fmt.Printf(string(node.value))
//	sl.delete(1)
//	sl.delete(2)
//	fmt.Printf(string(node.value))
//}