package App

import (
	"Projekat/App/BloomFilter"
	"Projekat/App/Cache"
	"Projekat/App/Memtable"
	"encoding/binary"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

type App struct {
	memtable *Memtable.Memtable
	cache    *Cache.Cache
	data     map[string]int
}

func CreateApp() App {
	app := App{}
	app.data = make(map[string]int)
	fromYaml := false
	yfile, err := ioutil.ReadFile("config.yaml")
	if err == nil {
		yaml.Unmarshal(yfile, &app.data)
		fromYaml = true
	}
	app.memtable = Memtable.CreateMemtable(app.data, fromYaml)
	app.cache = Cache.CreateCache(uint32(app.data["cache_max_size"]))
	return app
}

func (app *App) Put(key string, value []byte) {
	app.memtable.Write(key, value)
}

func (app *App) StopApp() {
	app.memtable.Finish()
}

func (app *App) Get(key string) (bool, []byte) {
	var value []byte
	var isThere bool
	isThere, value = app.memtable.Get(key)
	if isThere {
		fmt.Println("Nasao memtable")
		app.cache.AddElement(key, value)
		return true, value
	}

	isThere, value = app.cache.GetElement(key)
	if isThere {
		fmt.Println("Nasao cache")
		app.cache.AddElement(key, value)
		return true, value
	}

	fmt.Println(app.data["lsm_max_lvl"])
	for i := 1; i <= app.data["lsm_max_lvl"]; i++ {
		for j := 1; j <= Memtable.FindLSMGeneration(i); i++ {
			bloomFilter := BloomFilter.DeserializeBloomFilter(j, i)
			isThere = bloomFilter.IsElementInBloomFilter(key)
			if isThere {
				fmt.Println("Bloom kaze potencijalno jeste" + strconv.Itoa(i) + strconv.Itoa(j))
				fileSummary, _ := os.OpenFile("Data/summary/usertable-lvl="+strconv.Itoa(i)+"-gen="+strconv.Itoa(j)+"-Summary.db", os.O_RDONLY, 0777)

				firstSizeBytes := make([]byte, 8)
				fileSummary.Read(firstSizeBytes)
				firstSize := binary.LittleEndian.Uint64(firstSizeBytes)

				firstIndexBytes := make([]byte, firstSize)
				fileSummary.Read(firstIndexBytes)

				lastSizeBytes := make([]byte, 8)
				fileSummary.Read(lastSizeBytes)
				lastSize := binary.LittleEndian.Uint64(lastSizeBytes)

				lastIndexBytes := make([]byte, lastSize)
				fileSummary.Read(lastIndexBytes)

				fmt.Println(firstSize, string(firstIndexBytes), lastSize, string(lastIndexBytes))

				if key >= string(firstIndexBytes) && key <= string(lastIndexBytes) {
					fmt.Println("SSTable summary kaze da potencijalno jeste")
					for {
						keyLenBytes := make([]byte, 8)
						_, err := fileSummary.Read(keyLenBytes)
						if err == io.EOF {
							break
						}
						keyLen := binary.LittleEndian.Uint64(keyLenBytes)

						buff := make([]byte, keyLen+8)
						fileSummary.Read(buff)
						keyBytes := buff[:keyLen]
						indexPosition := binary.LittleEndian.Uint64(buff[keyLen:])
						if string(keyBytes) == key {
							fileSummary.Close()
							fmt.Println("SSTable nasao")
							fileIndex, _ := os.OpenFile("Data/index/usertable-lvl="+strconv.Itoa(i)+"-gen="+strconv.Itoa(j)+"-Index.db", os.O_RDONLY, 0777)
							fileIndex.Seek(int64(indexPosition), 0)

							keyLenIndexBytes := make([]byte, 8)
							fileIndex.Read(keyLenIndexBytes)
							keyLenIndex := binary.LittleEndian.Uint64(keyLenIndexBytes)

							buff2 := make([]byte, keyLenIndex+8)
							fileIndex.Read(buff2)
							dataPosition := binary.LittleEndian.Uint64(buff2[keyLenIndex:])

							fileIndex.Close()

							fileData, _ := os.OpenFile("Data/data/usertable-lvl="+strconv.Itoa(i)+"-gen="+strconv.Itoa(j)+"-Data.db", os.O_RDONLY, 0777)
							fileData.Seek(int64(dataPosition), 0)

							crc := make([]byte, 4)
							fileData.Read(crc)
							//c := binary.LittleEndian.Uint32(crc)
							//
							//if Memtable.CRC32(value) != c {
							//	panic("Nece da oce")
							//}

							fileData.Seek(8, 1)

							whatToDo := make([]byte, 1)
							fileData.Read(whatToDo)
							if whatToDo[0] == 1 {
								fmt.Println("Logicki obrisan")
								return false, []byte("Ne postoji")
							}

							keySize := make([]byte, 8)
							fileData.Read(keySize)
							n := binary.LittleEndian.Uint64(keySize)

							valueSize := make([]byte, 8)
							fileData.Read(valueSize)
							mm := binary.LittleEndian.Uint64(valueSize)

							keyData := make([]byte, n)
							fileData.Read(keyData)
							value = make([]byte, mm)
							fileData.Read(value)
							app.cache.AddElement(key, value)
							return true, value
						}
					}
				}
			}
		}
	}
	return false, []byte("Ne postoji")
}

func (app *App) Delete(key string, value []byte) bool {
	return app.memtable.Delete(key, value)
}
