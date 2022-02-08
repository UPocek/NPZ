package main

import (
	"Projekat/App/BloomFilter"
	"Projekat/App/CMS"
	"Projekat/App/Cache"
	"Projekat/App/HLL"
	"Projekat/App/MerkleTree"
	"Projekat/App/SkipList"
	"Projekat/App/TokenBucket"
	"Projekat/App/WAL"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Memtable struct {
	size         uint
	threshold    uint
	currentSize  uint
	skipList     skipList.SkipList
	cms          countMinSketch.CountMinSketch
	hll          hyperLogLog.HLL
	wal          *wal.WAL
	hashFunction hash.Hash32
}

func createMemtable() Memtable {
	memtable := Memtable{}
	insideWal := wal.CreateWAL()
	memtable.wal = &insideWal
	memtable.skipList = skipList.CreateSkipList()
	memtable.cms = countMinSketch.CreateCountMinSketch(0.01, 0.01)
	memtable.hll = hyperLogLog.CreateHLL(4)

	ts := uint(time.Now().Unix())
	memtable.hashFunction = murmur3.New32WithSeed(uint32(ts))

	yfile, err := ioutil.ReadFile("memtable/config.yaml")
	if err != nil {
		memtable.wal.SetSize(10)
		memtable.size = 10
		memtable.threshold = 80
	} else {
		data := make(map[string]int)
		err1 := yaml.Unmarshal(yfile, &data)
		if err1 != nil {
			panic(err1)
		}
		memtable.wal.SetSize(data["wal_size"])
		memtable.size = uint(data["memtable_size"])
		memtable.threshold = uint(data["threshold"])
	}
	return memtable
}

func (m Memtable) Write(key string, value []byte) bool {

	success := m.wal.AddElement(key, value)
	if success {
		m.cms.AddElement(key)
		m.hll.AddElement(key)
		m.currentSize += 1
		m.hashFunction.Reset()
		m.hashFunction.Write([]byte(key))
		i := m.hashFunction.Sum32()
		err := m.skipList.AddElement(float64(i), value)
		if err != nil {
			return false
		}
		if m.currentSize*100 >= m.size*m.threshold {
			m.Flush()
		}
		return true
	}
	return false
}

func (m Memtable) Delete(key string, value []byte) bool {
	success := m.wal.DeleteElement(key, value)
	if success {
		m.hashFunction.Reset()
		m.hashFunction.Write([]byte(key))
		i := m.hashFunction.Sum32()
		err := m.skipList.RemoveElement(float64(i))
		if err != nil {
			return false
		}
		return true
	}
	return false
}

func (m Memtable) Flush() {
	gen := findCurrentGeneration()
	elements := make([]*skipList.SkipListNode, m.currentSize, m.currentSize)
	first := m.skipList.GetHead()
	for first.GetDown() != nil {
		first = first.GetDown()
	}
	for first.GetRight() != nil {
		elements = append(elements, first.GetRight())
		first = first.GetRight()
	}

	merkle := merkleTree.MerkleTree{}
	bloom := bloomFilter.CreateBloomFilter(len(elements), 0.01)
	for _, el := range elements {
		merkle.AddElement(el.GetValue())
		bloom.AddElement(string(el.GetValue()))
	}
	// usertable-GEN-Filter.db
	bloom.SerializeBloomFilter(gen + 1)
	// usertable-GEN-Metadata.db
	merkle.CreateTree()
	merkle.SerializeTree(gen + 1)

	// usertable-GEN-Data.db
	// usertable-GEN-Index.db
	// usertable-GEN-Summary.db
	CreateSSTable(elements, gen+1)

	// usertable-GEN-TOC.txt
	file, err := os.OpenFile("toc/usertable-"+strconv.Itoa(gen+1)+"-TOC.txt", os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	file.Write([]byte("bloomFilter/usertable-" + strconv.Itoa(gen+1) + "-Filter.db\nmerkleTree/usertable-" + strconv.Itoa(gen+1) + "-Metadata.db\ndata/usertable-" + strconv.Itoa(gen+1) + "-Data.db\nindex/usertable-" + strconv.Itoa(gen+1) + "-Index.db\nsummary/usertable-" + strconv.Itoa(gen+1) + "-Summary.db\ntoc/usertable-" + strconv.Itoa(gen+1) + "-TOC.txt\n"))
	file.Close()

	m.skipList = skipList.CreateSkipList()
	m.wal.ResetWAL()
}

func CreateSSTable(elements []*skipList.SkipListNode, gen int) {
	var offset uint64 = 0
	var indexOffset uint64 = 0
	fileData, err1 := os.OpenFile("data/usertable-"+strconv.Itoa(gen+1)+"-Data.db", os.O_WRONLY|os.O_CREATE, 0777)
	if err1 != nil {
		panic(err1)
	}
	fileIndex, err2 := os.OpenFile("index/usertable-"+strconv.Itoa(gen+1)+"-Index.db", os.O_WRONLY|os.O_CREATE, 0777)
	if err2 != nil {
		panic(err2)
	}
	fileSummary, err3 := os.OpenFile("summary/usertable-"+strconv.Itoa(gen+1)+"-Summary.db", os.O_WRONLY|os.O_CREATE, 0777)
	if err3 != nil {
		panic(err3)
	}

	first := elements[0].GetKey()
	key_final1 := make([]byte, 8)
	ukey1 := math.Float64bits(first)
	binary.LittleEndian.PutUint64(key_final1, ukey1)
	last := elements[len(elements)-1].GetKey()
	key_final2 := make([]byte, 8)
	ukey2 := math.Float64bits(last)
	binary.LittleEndian.PutUint64(key_final2, ukey2)
	fileSummary.Write(key_final1)
	fileSummary.Write(key_final2)

	for _, element := range elements {
		// START - write to data
		crc := CRC32(element.GetValue())
		crc_final := make([]byte, 4)
		binary.LittleEndian.PutUint32(crc_final, crc)

		timestamp_final := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestamp_final, uint64(element.GetTimestamp()))

		tombstone_final := make([]byte, 1)
		if element.GetTombstone() {
			tombstone_final[0] = 1
		}

		var keySize uint64 = uint64(unsafe.Sizeof(element.GetKey()))
		keySize_final := make([]byte, 8)
		binary.LittleEndian.PutUint64(keySize_final, keySize)

		var valueSize uint64 = uint64(len(element.GetValue()))
		valueSize_final := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueSize_final, valueSize)

		// func Float64frombits(b uint64) float64
		// https://pkg.go.dev/math#Float64bits
		key_final := make([]byte, keySize)
		ukey := math.Float64bits(element.GetKey())
		binary.LittleEndian.PutUint64(key_final, ukey)

		fileData.Write(crc_final)
		fileData.Write(timestamp_final)
		fileData.Write(tombstone_final)
		fileData.Write(keySize_final)
		fileData.Write(valueSize_final)
		fileData.Write(key_final)
		fileData.Write(element.GetValue())
		recordSize := 4 + 8 + 1 + 8 + 8 + keySize + valueSize
		// END - write to data

		// START - write to index
		offset_final := make([]byte, 8)
		binary.LittleEndian.PutUint64(offset_final, offset)
		fileIndex.Write(key_final)
		fileIndex.Write(offset_final)
		offset += recordSize
		indexSize := keySize + 8
		// END - write to index

		// START - write summary elements (borders already written)
		fileSummary.Write(key_final)
		index_offset_final := make([]byte, 8)
		binary.LittleEndian.PutUint64(offset_final, indexOffset)
		fileSummary.Write(index_offset_final)
		indexOffset += indexSize
		// END - write summary elements

	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func findCurrentGeneration() int {
	files, _ := ioutil.ReadDir("data")
	maxName := 0
	for _, f := range files {
		str := f.Name()
		x := strings.Split(str, "-Data.db")
		x = strings.Split(x[0], "usertable-")
		num, _ := strconv.Atoi(x[1])
		if num > maxName {
			maxName = num
		}
	}
	return maxName
}

func (m Memtable) Finish() {
	m.wal.Finish()
}

func main() {

	memtable := createMemtable()
	fmt.Println(memtable)
	memtable.Finish()

}
