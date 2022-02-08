package Cache

import (
	"fmt"
)

type ListNode struct {
	key   string
	value []byte
	next  *ListNode
	prev  *ListNode
}

type DoublyLinkedList struct {
	head *ListNode
	tail *ListNode
}

type Cache struct {
	maxSize     uint32
	currentSize uint32
	list        DoublyLinkedList
	hashMap     map[string][]byte
}

func CreateCache(maxSize uint32) *Cache {
	cache := Cache{}
	cache.maxSize = maxSize
	cache.list = DoublyLinkedList{
		head: &ListNode{
			key:   "",
			value: nil,
			next:  nil,
			prev:  nil,
		},
		tail: &ListNode{
			key:   "",
			value: nil,
			next:  nil,
			prev:  nil,
		},
	}
	cache.list.head.next = cache.list.tail
	cache.list.tail.prev = cache.list.head
	cache.hashMap = make(map[string][]byte, cache.maxSize)
	return &cache
}

func (cache *Cache) AddElement(key string, value []byte) bool {
	_, isHere := cache.hashMap[key]
	if isHere {
		//premesti na prvi element liste
		cache.list.setOnFirst(key)
	} else {
		//dodaj u listu
		cache.hashMap[key] = value
		cache.currentSize++
		cache.list.addElement(key, value)
	}
	// ako je preslo makismum izbaci poslednji
	if cache.currentSize > cache.maxSize {
		// izbaci poslednji
		k := cache.getLast().key
		cache.list.removeLast()
		cache.currentSize--
		delete(cache.hashMap, k)
	}
	return true
}

func (cache *Cache) RemoveElement(key string) bool {
	_, isHere := cache.hashMap[key]
	if isHere {
		cache.currentSize--
		delete(cache.hashMap, key)
		cache.list.removeElement(key)
		return true
	}
	return false
}

func (cache *Cache) GetElement(key string) (bool, []byte) {
	value, isHere := cache.hashMap[key]
	if isHere {
		cache.list.setOnFirst(key)
		return true, value
	}
	return false, nil
}

func (cache *Cache) printCache() {
	cache.list.printList()
}

func (cache *Cache) getLast() *ListNode {
	return cache.list.tail.prev
}

func (cache *Cache) SetMaxSize(maxSize uint32) {
	cache.maxSize = maxSize
}

func (dll *DoublyLinkedList) printList() {
	currNode := dll.head.next
	for currNode != nil {
		fmt.Println(currNode.key)
		currNode = currNode.next
	}
}

func (dll *DoublyLinkedList) setOnFirst(key string) {
	currNode := dll.head.next
	var node *ListNode
	for currNode != nil {
		if currNode.key == key {
			node = currNode
			break
		}
		currNode = currNode.next
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}

	node.next = dll.head.next
	dll.head.next.prev = node
	dll.head.next = node
	node.prev = dll.head
}

func (dll *DoublyLinkedList) setOnLast(key string) {
	currNode := dll.head.next
	var node *ListNode
	for currNode != nil {
		if currNode.key == key {
			node = currNode
			break
		}
		currNode = currNode.next
	}

	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}

	node.prev = dll.tail.prev
	dll.tail.prev.next = node
	dll.tail.prev = node
	node.next = dll.tail

}

func (dll *DoublyLinkedList) addElement(key string, value []byte) {
	node := ListNode{
		key:   key,
		value: value,
		next:  dll.head.next,
		prev:  dll.head,
	}
	dll.head.next.prev = &node
	dll.head.next = &node
}

func (dll *DoublyLinkedList) removeLast() {
	dll.tail.prev.prev.next = dll.tail
	temp := dll.tail.prev
	dll.tail.prev = dll.tail.prev.prev
	temp.next = nil
	temp.prev.prev = nil
}

func (dll *DoublyLinkedList) removeElement(key string) {
	dll.setOnLast(key)
	dll.removeLast()
}

func main() {
	cache := CreateCache(5)
	cache.AddElement("1", []byte("Test"))
	cache.AddElement("2", []byte("Test"))
	cache.AddElement("3", []byte("Test"))
	cache.AddElement("4", []byte("Test"))
	cache.AddElement("5", []byte("KKKKK"))
	cache.printCache()
	cache.AddElement("6", []byte("Test"))
	cache.printCache()
	cache.AddElement("3", []byte("Test"))
	cache.printCache()
	fmt.Println(cache.GetElement("5"))
	cache.printCache()
	cache.RemoveElement("40")
	cache.RemoveElement("4")
	cache.RemoveElement("3")
	cache.AddElement("1", []byte("Test"))
	cache.AddElement("6", []byte("Test"))
	cache.printCache()
}
