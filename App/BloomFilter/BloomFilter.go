package BloomFilter

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"strconv"
	"time"
)

type BloomFilter struct {
	M             uint
	K             uint
	Ts            uint
	hashFunctions []hash.Hash32
	BitSet        []int
}

func CreateBloomFilter(expectedElements int, falsePositiveRate float64) BloomFilter {
	b := BloomFilter{}

	b.M = calculateM(expectedElements, falsePositiveRate)
	b.K = calculateK(expectedElements, b.M)
	b.hashFunctions, b.Ts = createHashFunctions(b.K, 0)
	b.createBitSet()

	return b
}

func (b *BloomFilter) createBitSet() {
	b.BitSet = make([]int, b.M, b.M)
}

func (b *BloomFilter) AddElement(element string) {

	for j := 0; j < len(b.hashFunctions); j++ {
		b.hashFunctions[j].Reset()
		b.hashFunctions[j].Write([]byte(element))
		i := b.hashFunctions[j].Sum32() % uint32(b.M)
		b.BitSet[i] = 1
	}

}

func (b *BloomFilter) IsElementInBloomFilter(element string) bool {
	for j := 0; j < len(b.hashFunctions); j++ {
		b.hashFunctions[j].Reset()
		b.hashFunctions[j].Write([]byte(element))
		i := b.hashFunctions[j].Sum32() % uint32(b.M)
		if b.BitSet[i] == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) SerializeBloomFilter(gen int) {
	file, err := os.Create("Data/bloomFilter/usertable-" + strconv.Itoa(gen) + "-Filter.db")
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(b)
	if err != nil {
		panic(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}
}

func DeserializeBloomFilter(gen int) BloomFilter {
	file, err := os.OpenFile("Data/bloomFilter/usertable-"+strconv.Itoa(gen)+"-Filter.db", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	newB := BloomFilter{}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&newB)
	if err != nil {
		panic(err)
	}
	newB.hashFunctions, _ = createHashFunctions(newB.K, newB.Ts)
	err = file.Close()
	if err != nil {
		panic(err)
	}
	return newB
}

func calculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func calculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func createHashFunctions(k uint, ts uint) ([]hash.Hash32, uint) {
	h := []hash.Hash32{}
	if ts == 0 {
		ts = uint(time.Now().Unix())
	}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h, ts
}

func main() {

	b1 := CreateBloomFilter(100, 0.05)
	b1.AddElement("Pepermint")
	b1.AddElement("Sun")
	b1.AddElement("Ice")
	b1.AddElement("Happy")
	fmt.Println(b1.IsElementInBloomFilter("Pepermint"))

}
