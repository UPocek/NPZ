package App

import (
	"Projekat/App/BloomFilter"
	"Projekat/App/Cache"
	"Projekat/App/Memtable"
	"Projekat/App/TokenBucket"
	"encoding/binary"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

type App struct {
	memtable    *Memtable.Memtable
	cache       *Cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	data        map[string]int
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
	app.tokenBucket = TokenBucket.CreateTokenBucket(app.data["tokenbucket_size"], app.data["tokenbucket_interval"])
	return app
}

func (app *App) Put(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		ok := app.memtable.Write(key, value)
		return ok
	}
	fmt.Println("Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) Get(key string) (bool, []byte) {
	if app.tokenBucket.Update() {
		var value []byte
		var isThere, deleted bool
		isThere, deleted, value = app.memtable.Get(key)
		if isThere {
			if deleted {
				return false, []byte("Podatak je logicki obrisan")
			} else {
				app.cache.AddElement(key, value)
				return true, value
			}

		}

		isThere, value = app.cache.GetElement(key)
		if isThere {
			app.cache.AddElement(key, value)
			return true, value
		}

		for i := 1; i <= app.data["lsm_max_lvl"]; i++ {
			maxGen := Memtable.FindLSMGeneration(i)
			for j := 1; j <= maxGen; j++ {

				gen := j
				if i == app.data["lsm_max_lvl"] {
					j = maxGen - j + 1
				}
				bloomFilter := BloomFilter.DeserializeBloomFilter(j, i)
				isThere = bloomFilter.IsElementInBloomFilter(key)
				if isThere {

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

					if key >= string(firstIndexBytes) && key <= string(lastIndexBytes) {
						summeryStructure := make(map[string]uint64)
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
							summeryStructure[string(keyBytes)] = indexPosition
						}

						indexPosition, existInMap := summeryStructure[key]
						if existInMap {

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
							c := binary.LittleEndian.Uint32(crc)

							fileData.Seek(8, 1)

							whatToDo := make([]byte, 1)
							fileData.Read(whatToDo)
							if whatToDo[0] == 1 {

								fileSummary.Close()
								fileData.Close()

								return false, []byte("Podatak je logicki obrisan")
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
							if Memtable.CRC32(value) != c {
								panic("Nece da oce")
							}
							fileSummary.Close()
							fileData.Close()
							app.cache.AddElement(key, value)
							fileData.Close()
							fileSummary.Close()
							return true, value
						}
					}
					fileSummary.Close()
				}
				j = gen
			}
		}
		return false, []byte("Ne postoji")
	}
	return false, []byte("Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
}

func (app *App) Delete(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		answer, _ := app.Get(key)
		app.cache.RemoveElement(key)
		return app.memtable.Delete(key, value, answer)
	}
	fmt.Println("Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) StopApp() {
	app.memtable.Finish()
}

func (app *App) Test() bool {
	app.Put("kljuc01", []byte("vrednost1"))
	app.Put("kljuc02", []byte("vrednost2"))
	app.Put("kljuc03", []byte("vrednost3"))
	app.Put("kljuc04", []byte("vrednost4"))
	app.Put("kljuc05", []byte("vrednost5"))
	app.Put("kljuc06", []byte("vrednost6"))
	app.Put("kljuc07", []byte("vrednost7"))

	app.Put("kljuc08", []byte("vrednost8"))
	app.Delete("kljuc03", []byte("vrednost3"))
	app.Put("kljuc10", []byte("vrednost10"))
	app.Put("kljuc11", []byte("vrednost11"))
	app.Put("kljuc12", []byte("vrednost12"))
	app.Put("kljuc13", []byte("vrednost13"))
	app.Put("kljuc14", []byte("vrednost14"))

	app.Delete("kljuc15", []byte("vrednost15"))
	app.Delete("kljuc05", []byte("vrednost5"))
	app.Delete("kljuc06", []byte("vrednost6"))
	app.Put("kljuc18", []byte("vrednost18"))
	app.Delete("kljuc08", []byte("vrednost08"))
	app.Put("kljuc20", []byte("vrednost20"))
	app.Put("kljuc21", []byte("vrednost21"))
	app.Put("kljuc30", []byte("vrednost30"))

	app.Put("kljuc23", []byte("vrednost23"))
	app.Put("kljuc24", []byte("vrednost24"))
	app.Put("kljuc25", []byte("vrednost25"))
	app.Put("kljuc26", []byte("vrednost26"))
	app.Put("kljuc27", []byte("vrednost27"))
	app.Put("kljuc01", []byte("vrednost01"))
	app.Put("kljuc28", []byte("vrednost28"))

	app.Put("kljuc31", []byte("vrednost31"))
	app.Put("kljuc32", []byte("vrednost32"))
	app.Put("kljuc33", []byte("vrednost33"))
	app.Put("kljuc34", []byte("vrednost34"))
	app.Put("kljuc35", []byte("vrednost35"))
	app.Put("kljuc36", []byte("vrednost36"))
	app.Put("kljuc37", []byte("vrednost37"))

	app.Put("kljuc08", []byte("vrednost8"))
	app.Delete("kljuc07", []byte("vrednost7"))
	app.Put("kljuc10", []byte("vrednost10"))
	app.Put("kljuc11", []byte("vrednost11"))
	app.Put("kljuc12", []byte("vrednost12"))
	app.Put("kljuc13", []byte("vrednost13"))
	app.Put("kljuc14", []byte("vrednost14"))

	app.Delete("kljuc15", []byte("vrednost15"))
	app.Delete("kljuc05", []byte("vrednost5"))
	app.Delete("kljuc06", []byte("vrednost6"))
	app.Put("kljuc18", []byte("vrednost18"))
	app.Delete("kljuc08", []byte("vrednost08"))
	app.Put("kljuc20", []byte("vrednost20"))
	app.Put("kljuc21", []byte("vrednost21"))
	app.Put("kljuc30", []byte("vrednost30"))

	app.Put("kljuc23", []byte("vrednost23"))
	app.Put("kljuc24", []byte("vrednost24"))
	app.Put("kljuc25", []byte("vrednost25"))
	app.Put("kljuc26", []byte("vrednost26"))
	app.Put("kljuc27", []byte("vrednost27"))
	app.Put("kljuc01", []byte("vrednost01"))
	app.Put("kljuc28", []byte("vrednost28"))

	app.Put("kljuc01", []byte("vrednost1"))
	app.Put("kljuc02", []byte("nova_vrednost2"))
	app.Put("kljuc03", []byte("vrednost3"))
	app.Put("kljuc04", []byte("vrednost4"))
	app.Put("kljuc05", []byte("vrednost5"))
	app.Put("kljuc06", []byte("vrednost6"))
	app.Put("kljuc07", []byte("vrednost7"))

	app.Put("kljuc08", []byte("vrednost8"))
	app.Delete("kljuc03", []byte("vrednost3"))
	app.Put("kljuc10", []byte("vrednost10"))
	app.Put("kljuc11", []byte("vrednost11"))
	app.Put("kljuc12", []byte("vrednost12"))
	app.Put("kljuc13", []byte("vrednost13"))
	app.Put("kljuc14", []byte("vrednost14"))

	app.Delete("kljuc15", []byte("vrednost15"))
	app.Delete("kljuc05", []byte("vrednost5"))
	app.Delete("kljuc06", []byte("vrednost6"))
	app.Put("kljuc18", []byte("vrednost18"))
	app.Delete("kljuc08", []byte("vrednost08"))
	app.Put("kljuc20", []byte("vrednost20"))
	app.Put("kljuc21", []byte("vrednost21"))
	app.Put("kljuc30", []byte("vrednost30"))

	app.Put("kljuc23", []byte("vrednost23"))
	app.Put("kljuc24", []byte("vrednost24"))
	app.Put("kljuc25", []byte("vrednost25"))
	app.Put("kljuc26", []byte("vrednost26"))
	app.Put("kljuc27", []byte("vrednost27"))
	app.Put("kljuc01", []byte("vrednost01"))
	app.Put("kljuc28", []byte("vrednost28"))

	_, value := app.Get("kljuc01")
	fmt.Println(string(value))
	_, value = app.Get("kljuc02")
	fmt.Println(string(value))
	_, value = app.Get("kljuc03")
	fmt.Println(string(value))
	_, value = app.Get("kljuc04")
	fmt.Println(string(value))
	_, value = app.Get("kljuc05")
	fmt.Println(string(value))
	_, value = app.Get("kljuc06")
	fmt.Println(string(value))
	_, value = app.Get("kljuc07")
	fmt.Println(string(value))
	_, value = app.Get("kljuc08")
	fmt.Println(string(value))
	_, value = app.Get("kljuc09")
	fmt.Println(string(value))
	_, value = app.Get("kljuc10")
	fmt.Println(string(value))
	_, value = app.Get("kljuc11")
	fmt.Println(string(value))
	_, value = app.Get("kljuc12")
	fmt.Println(string(value))
	_, value = app.Get("kljuc13")
	fmt.Println(string(value))
	_, value = app.Get("kljuc14")
	fmt.Println(string(value))
	_, value = app.Get("kljuc15")
	fmt.Println(string(value))
	_, value = app.Get("kljuc16")
	fmt.Println(string(value))
	_, value = app.Get("kljuc17")
	fmt.Println(string(value))
	_, value = app.Get("kljuc18")
	fmt.Println(string(value))
	_, value = app.Get("kljuc19")
	fmt.Println(string(value))
	_, value = app.Get("kljuc20")
	fmt.Println(string(value))
	_, value = app.Get("kljuc21")
	fmt.Println(string(value))
	_, value = app.Get("kljuc22")
	fmt.Println(string(value))
	_, value = app.Get("kljuc23")
	fmt.Println(string(value))
	_, value = app.Get("kljuc24")
	fmt.Println(string(value))
	_, value = app.Get("kljuc25")
	fmt.Println(string(value))
	_, value = app.Get("kljuc26")
	fmt.Println(string(value))
	_, value = app.Get("kljuc27")
	fmt.Println(string(value))
	_, value = app.Get("kljuc28")
	fmt.Println(string(value))
	_, value = app.Get("kljuc29")
	fmt.Println(string(value))
	_, value = app.Get("kljuc30")
	fmt.Println(string(value))
	_, value = app.Get("kljuc31")
	fmt.Println(string(value))
	_, value = app.Get("kljuc32")
	fmt.Println(string(value))
	_, value = app.Get("kljuc33")
	fmt.Println(string(value))
	_, value = app.Get("kljuc34")
	fmt.Println(string(value))
	_, value = app.Get("kljuc35")
	fmt.Println(string(value))
	_, value = app.Get("kljuc36")
	fmt.Println(string(value))
	_, value = app.Get("kljuc37")
	fmt.Println(string(value))

	return true
}
