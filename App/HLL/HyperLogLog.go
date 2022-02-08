package HLL

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
	"os"
	"strconv"
	"time"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M            uint32 //duzina niza
	P            uint8  //preciznost
	Ts           uint
	Reg          []uint8 // baketi
	hashFunction hash.Hash32
}

func CreateHLL(p uint8) *HLL {
	hll := HLL{P: p}

	hll.M, hll.Reg = createBuckets(hll.P)
	hll.hashFunction, hll.Ts = createHashFunction()

	return &hll
}

func (hll *HLL) AddElement(key string) {
	hll.hashFunction.Reset()
	hll.hashFunction.Write([]byte(key))
	i := hll.hashFunction.Sum32()
	n := bits.TrailingZeros32(i)
	i = i >> (32 - hll.P)

	hll.Reg[i] = uint8(n)

}

func createBuckets(p uint8) (uint32, []uint8) {
	m := uint32(math.Pow(2, float64(p)))
	reg := make([]uint8, m)
	return m, reg
}

func createHashFunction() (hash.Hash32, uint) {
	ts := uint(time.Now().Unix())
	hashFunction := murmur3.New32WithSeed(uint32(ts))
	return hashFunction, ts
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll HLL) SerializeHLL(gen, lvl int) {
	file, err := os.Create("Data/hyperLogLog/usertable-lvl=" + strconv.Itoa(lvl) + "-gen=" + strconv.Itoa(gen) + "-HyperLogLog.db")
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(hll)
	if err != nil {
		panic(err)
	}
	file.Close()
}

func DeserializeHLL(gen, lvl int) HLL {
	file, err := os.OpenFile("Data/hyperLogLog/usertable-lvl="+strconv.Itoa(lvl)+"-gen="+strconv.Itoa(gen)+"-HyperLogLog.db", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	hll := HLL{}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&hll)
	if err != nil {
		panic(err)
	}
	hll.hashFunction = murmur3.New32WithSeed(uint32(hll.Ts))
	file.Close()
	return hll
}

func main() {

	myHLL := CreateHLL(4)

	myHLL.AddElement("Sun")
	myHLL.AddElement("Ice")
	myHLL.AddElement("Pepermint")
	myHLL.AddElement("Cat")
	myHLL.AddElement("Number")

	fmt.Println(myHLL.Estimate())

}
