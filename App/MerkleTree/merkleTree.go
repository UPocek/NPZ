package MerkleTree

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type MerkleTree struct {
	root     *Node
	elements [][]byte
	leaves   []*Node
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func (mr *MerkleTree) AddElement(el []byte) {
	if mr.elements == nil {
		mr.elements = make([][]byte, 0, 9)
	}
	mr.elements = append(mr.elements, el)
}

func (mr *MerkleTree) CreateTree() {
	queue := make([]*Node, 0, 9)
	for _, el := range mr.elements {
		key := Hash(el)
		newNode := Node{
			data:  key,
			left:  nil,
			right: nil,
		}
		mr.leaves = append(mr.leaves, &newNode)
		queue = append(queue, &newNode)
	}
	if len(mr.leaves)%4 != 0 {
		d := 0
		for (len(mr.leaves)+d)%4 != 0 {
			key := Hash(make([]byte, 0, 0))
			newNode := Node{
				data:  key,
				left:  nil,
				right: nil,
			}
			queue = append(queue, &newNode)
			d += 1
		}
	}
	for len(queue) > 1 {
		leftN := queue[0]
		rightN := queue[1]
		newData := make([]byte, 0, 40)
		newData = append(newData, leftN.data[:]...)
		newData = append(newData, rightN.data[:]...)
		queue = append(queue[:0], queue[2:]...)
		newNode := Node{
			data:  Hash(newData),
			left:  leftN,
			right: rightN,
		}
		queue = append(queue, &newNode)
	}
	mr.root = queue[0]
}

func (mr *MerkleTree) String() string {
	return mr.root.String()
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func (mr *MerkleTree) SerializeTree(gen, lvl int) {
	file, err := os.OpenFile("Data/merkleTree/usertable-lvl="+strconv.Itoa(lvl)+"-gen="+strconv.Itoa(gen)+"-Metadata.db", os.O_WRONLY|os.O_CREATE, 0777)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	queue := make([]*Node, 0, 9)
	queue = append(queue, mr.root)
	for len(queue) > 0 {
		el := queue[0]
		if len(queue) == 1 {
			queue = queue[:0]
		} else {
			queue = append(queue[:0], queue[1:]...)
		}
		file.Write([]byte(el.String()))
		file.Write([]byte("|"))
		if el.left != nil {
			queue = append(queue, el.left)
		}
		if el.right != nil {
			queue = append(queue, el.right)
		}
	}
}

func ReconstructTree(gen, lvl int) MerkleTree {
	file, err := os.OpenFile("Data/merkleTree/usertable-lvl="+strconv.Itoa(lvl)+"-gen="+strconv.Itoa(gen)+"-Metadata.db", os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	content, err := ioutil.ReadAll(file)

	keys := strings.Split(string(content), "|")
	keys = keys[:len(keys)-1]

	newMerkleTree := MerkleTree{}

	nodes := make([]Node, len(keys), len(keys))

	for i := 0; i < len(keys); i++ {
		u, _ := hex.DecodeString(keys[i])
		var d [20]byte
		for j := 0; j < 20; j++ {
			d[j] = u[j]
		}
		nodes[i] = Node{
			data:  d,
			left:  nil,
			right: nil,
		}
	}
	i := 1
	newMerkleTree.root = &nodes[0]
	queue := make([]*Node, 0, 9)
	queue = append(queue, newMerkleTree.root)
	for len(queue) > 0 {
		el := queue[0]
		if len(queue) == 1 {
			queue = queue[:0]
		} else {
			queue = append(queue[:0], queue[1:]...)
		}
		if i < len(keys) {
			el.left = &nodes[i]
			i += 1
			el.right = &nodes[i]
			i += 1
			queue = append(queue, el.left)
			queue = append(queue, el.right)
		}
	}
	return newMerkleTree
}

func main() {

	MyMerkleTree := MerkleTree{}
	MyMerkleTree.AddElement([]byte("Test"))
	MyMerkleTree.AddElement([]byte("Super"))
	MyMerkleTree.AddElement([]byte("Radi"))
	MyMerkleTree.AddElement([]byte("Jej"))
	MyMerkleTree.AddElement([]byte("Ok1"))
	MyMerkleTree.AddElement([]byte("Ok2"))
	MyMerkleTree.AddElement([]byte("Ok3"))
	MyMerkleTree.AddElement([]byte("Ok4"))

	MyMerkleTree.CreateTree()

	fmt.Println(MyMerkleTree)

	MyMerkleTree.SerializeTree(1, 1)

	MyMerkleTree = ReconstructTree(1, 1)

	fmt.Println(MyMerkleTree)

}
