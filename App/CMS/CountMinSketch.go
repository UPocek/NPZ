package CMS

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

type CountMinSketch struct {
	M             uint
	K             uint
	Ts            uint
	hashFunctions []hash.Hash32
	Matrix        [][]uint
}

func CreateCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	cms := CountMinSketch{}

	cms.M = calculateM(epsilon)
	cms.K = calculateK(delta)
	cms.hashFunctions, cms.Ts = createHashFunctions(cms.K, 0)
	cms.Matrix = createMatrix(cms.K, cms.M)

	return &cms
}

func createMatrix(k uint, m uint) [][]uint {
	matrix := make([][]uint, k)
	for i := range matrix {
		matrix[i] = make([]uint, m)
	}
	return matrix
}

func (cms *CountMinSketch) AddElement(element string) {

	for j := 0; j < int(cms.K); j++ {
		cms.hashFunctions[j].Reset()
		cms.hashFunctions[j].Write([]byte(element))
		i := cms.hashFunctions[j].Sum32() % uint32(cms.M)
		cms.Matrix[j][i] += 1
	}

}

func (cms *CountMinSketch) FrequencyOfElement(element string) uint {

	a := make([]uint, cms.K, cms.K)
	for j := 0; j < int(cms.K); j++ {
		cms.hashFunctions[j].Reset()
		cms.hashFunctions[j].Write([]byte(element))
		i := cms.hashFunctions[j].Sum32() % uint32(cms.M)
		a[j] = cms.Matrix[j][i]
	}

	min := a[0]
	for k := 1; k < len(a); k++ {
		if a[k] < min {
			min = a[k]
		}
	}

	return min

}

func (cms CountMinSketch) SerializeCountMinSketch(gen, lvl int) {
	file, err := os.Create("Data/countMinSketch/usertable-lvl=" + strconv.Itoa(lvl) + "-gen=" + strconv.Itoa(gen) + "-CountMinSketch.db")
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)
	if err != nil {
		panic(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}
}

func DeserializeCountMinSketch(gen, lvl int) CountMinSketch {
	file, err := os.OpenFile("Data/countMinSketch/usertable-lvl="+strconv.Itoa(lvl)+"-gen="+strconv.Itoa(gen)+"-CountMinSketch.db", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	cms := CountMinSketch{}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&cms)
	if err != nil {
		panic(err)
	}
	cms.hashFunctions, cms.Ts = createHashFunctions(cms.K, cms.Ts)
	file.Close()
	return cms
}

func calculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func calculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func createHashFunctions(k, ts uint) ([]hash.Hash32, uint) {
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

	cms1 := CreateCountMinSketch(0.01, 0.01)
	cms1.AddElement("Pepermint")
	fmt.Println(cms1.FrequencyOfElement("Pepermint"))

}
