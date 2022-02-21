package App

import (
	"Projekat/App/BloomFilter"
	"Projekat/App/Cache"
	"Projekat/App/Memtable"
	"Projekat/App/TokenBucket"
	"bufio"
	"encoding/binary"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type User struct {
	username    string
	password    string
	tokenBucket *TokenBucket.TokenBucket
}

type App struct {
	memtable    *Memtable.Memtable
	cache       *Cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	data        map[string]int
	user        User
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

func (app *App) put(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		ok := app.memtable.Write(key, value)
		return ok
	}
	fmt.Println("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) get(key string) (bool, []byte) {
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
	return false, []byte("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
}

func (app *App) delete(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		answer, _ := app.get(key)
		app.cache.RemoveElement(key)
		return app.memtable.Delete(key, value, answer)
	}
	fmt.Println("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) RunApp() {
	for true {
		fmt.Print("Izaberite jednu od opcija:\n 1) Login \n 2) Register \n 3) Exit \n>> ")
		var option string
		fmt.Scanln(&option)
		if strings.Replace(option, ")", "", 1) == "1" {
			fmt.Print("Unesite korisničko ime\n >> ")
			var username string
			fmt.Scanln(&username)

			fmt.Print("Unesite lozinku\n >> ")
			var password string
			fmt.Scanln(&password)

			if app.login(username, password) {
				app.options()
			}
		} else if strings.Replace(option, ")", "", 1) == "2" {
			fmt.Print("Unesite korisničko ime\n >> ")
			var username string
			fmt.Scanln(&username)

			fmt.Print("Unesite lozinku\n >> ")
			var password string
			fmt.Scanln(&password)

			if !app.login(username, password) {
				file, err := os.OpenFile("Data/users/users.csv", os.O_APPEND, 0777)
				if err != nil {
					panic(err)
				}

				_, err = file.WriteString(username + "," + password + "," + strconv.Itoa(app.data["tokenbucket_size"]) + "," + strconv.Itoa(int(time.Now().Unix())) + "\n")
				if err != nil {
					panic(err)
				}
				file.Close()
			} else {
				fmt.Println("\nGreška: Korisnik sa tim podacima već postoji.\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "3" {
			break
		} else {
			fmt.Println("\nGreška: Uneta opcija ne postoji pokušajte ponovo.\n")
		}
	}
}

func (app *App) login(username, password string) bool {
	file, err := os.OpenFile("Data/users/users.csv", os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		// do something with a line
		items := strings.Split(scanner.Text(), ",")
		if username == items[0] && password == items[1] {
			app.user.username = username
			app.user.password = password
			tokensLeft, _ := strconv.Atoi(items[2])
			lastReset, _ := strconv.Atoi(items[3])
			app.user.tokenBucket = TokenBucket.CreateTokenBucket(app.data["tokenbucket_size"], app.data["tokenbucket_interval"], tokensLeft, int64(lastReset))
			app.tokenBucket = app.user.tokenBucket
			return true
		}
	}
	return false
}

func (app *App) options() {
	for true {
		fmt.Print("Izaberite jednu od opcija:\n 1) Put \n 2) Get \n 3) Delete \n 4) Test \n 5) Logout \n >> ")
		var option string
		fmt.Scanln(&option)
		if strings.Replace(option, ")", "", 1) == "1" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			ok := app.put(key, []byte(value))
			if ok {
				fmt.Println("\nDodavanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nGreška: Prilikom dodavanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "2" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			ok, value := app.get(key)
			if ok {
				fmt.Println("\nVrednost ključa " + key + " je " + string(value) + "\n")
			} else {
				fmt.Println(string(value))
			}
		} else if strings.Replace(option, ")", "", 1) == "3" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			ok := app.delete(key, []byte(value))
			if ok {
				fmt.Println("\nBrisanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nGreška: Prilikom brisanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "4" {
			ok := app.test()
			if !ok {
				fmt.Println("\nGreška: Prilikom testiranja\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "5" {
			break
		} else {
			fmt.Println("\nGreška: Uneta opcija ne postoji pokušajte ponovo.\n")
		}
		app.updateUsersFile()
	}
}

func (app *App) updateUsersFile() {
	file, err := os.OpenFile("Data/users/users.csv", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	list := make([]byte, 0)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), app.user.username) {
			list = append(list, app.user.username+","+app.user.password+","+strconv.Itoa(app.user.tokenBucket.GetTokensLeft())+","+strconv.Itoa(int(app.user.tokenBucket.GetLastReset()))+"\n"...)
		} else {
			list = append(list, []byte(scanner.Text()+"\n")...)
		}
	}
	file.Close()

	file, err = os.OpenFile("Data/users/users.csv", os.O_RDWR, 0777)
	file.WriteString(string(list))
	file.Close()

}

func (app *App) StopApp() {
	app.memtable.Finish()
	os.Exit(0)
}

func (app *App) test() bool {
	app.put("kljuc01", []byte("vrednost1"))
	app.put("kljuc02", []byte("vrednost2"))
	app.put("kljuc03", []byte("vrednost3"))
	app.put("kljuc04", []byte("vrednost4"))
	app.put("kljuc05", []byte("vrednost5"))
	app.put("kljuc06", []byte("vrednost6"))
	app.put("kljuc07", []byte("vrednost7"))

	app.put("kljuc08", []byte("vrednost8"))
	app.delete("kljuc03", []byte("vrednost3"))
	app.put("kljuc10", []byte("vrednost10"))
	app.put("kljuc11", []byte("vrednost11"))
	app.put("kljuc12", []byte("vrednost12"))
	app.put("kljuc13", []byte("vrednost13"))
	app.put("kljuc14", []byte("vrednost14"))

	app.delete("kljuc15", []byte("vrednost15"))
	app.delete("kljuc05", []byte("vrednost5"))
	app.delete("kljuc06", []byte("vrednost6"))
	app.put("kljuc18", []byte("vrednost18"))
	app.delete("kljuc08", []byte("vrednost08"))
	app.put("kljuc20", []byte("vrednost20"))
	app.put("kljuc21", []byte("vrednost21"))
	app.put("kljuc30", []byte("vrednost30"))

	app.put("kljuc23", []byte("vrednost23"))
	app.put("kljuc24", []byte("vrednost24"))
	app.put("kljuc25", []byte("vrednost25"))
	app.put("kljuc26", []byte("vrednost26"))
	app.put("kljuc27", []byte("vrednost27"))
	app.put("kljuc01", []byte("vrednost01"))
	app.put("kljuc28", []byte("vrednost28"))

	app.put("kljuc31", []byte("vrednost31"))
	app.put("kljuc32", []byte("vrednost32"))
	app.put("kljuc33", []byte("vrednost33"))
	app.put("kljuc34", []byte("vrednost34"))
	app.put("kljuc35", []byte("vrednost35"))
	app.put("kljuc36", []byte("vrednost36"))
	app.put("kljuc37", []byte("vrednost37"))

	app.put("kljuc08", []byte("vrednost8"))
	app.delete("kljuc07", []byte("vrednost7"))
	app.put("kljuc10", []byte("vrednost10"))
	app.put("kljuc11", []byte("vrednost11"))
	app.put("kljuc12", []byte("vrednost12"))
	app.put("kljuc13", []byte("vrednost13"))
	app.put("kljuc14", []byte("vrednost14"))

	app.delete("kljuc15", []byte("vrednost15"))
	app.delete("kljuc05", []byte("vrednost5"))
	app.delete("kljuc06", []byte("vrednost6"))
	app.put("kljuc18", []byte("vrednost18"))
	app.delete("kljuc08", []byte("vrednost08"))
	app.put("kljuc20", []byte("vrednost20"))
	app.put("kljuc21", []byte("vrednost21"))
	app.put("kljuc30", []byte("vrednost30"))

	app.put("kljuc23", []byte("vrednost23"))
	app.put("kljuc24", []byte("vrednost24"))
	app.put("kljuc25", []byte("vrednost25"))
	app.put("kljuc26", []byte("vrednost26"))
	app.put("kljuc27", []byte("vrednost27"))
	app.put("kljuc01", []byte("vrednost01"))
	app.put("kljuc28", []byte("vrednost28"))

	app.put("kljuc01", []byte("vrednost1"))
	app.put("kljuc02", []byte("nova_vrednost2"))
	app.put("kljuc03", []byte("vrednost3"))
	app.put("kljuc04", []byte("vrednost4"))
	app.put("kljuc05", []byte("vrednost5"))
	app.put("kljuc06", []byte("vrednost6"))
	app.put("kljuc07", []byte("vrednost7"))

	app.put("kljuc08", []byte("vrednost8"))
	app.delete("kljuc03", []byte("vrednost3"))
	app.put("kljuc10", []byte("vrednost10"))
	app.put("kljuc11", []byte("vrednost11"))
	app.put("kljuc12", []byte("vrednost12"))
	app.put("kljuc13", []byte("vrednost13"))
	app.put("kljuc14", []byte("vrednost14"))

	app.delete("kljuc15", []byte("vrednost15"))
	app.delete("kljuc05", []byte("vrednost5"))
	app.delete("kljuc06", []byte("vrednost6"))
	app.put("kljuc18", []byte("vrednost18"))
	app.delete("kljuc08", []byte("vrednost08"))
	app.put("kljuc20", []byte("vrednost20"))
	app.put("kljuc21", []byte("vrednost21"))
	app.put("kljuc30", []byte("vrednost30"))

	app.put("kljuc23", []byte("vrednost23"))
	app.put("kljuc24", []byte("vrednost24"))
	app.put("kljuc25", []byte("vrednost25"))
	app.put("kljuc26", []byte("vrednost26"))
	app.put("kljuc27", []byte("vrednost27"))
	app.put("kljuc01", []byte("vrednost01"))
	app.put("kljuc28", []byte("vrednost28"))

	_, value := app.get("kljuc01")
	fmt.Println(string(value))
	_, value = app.get("kljuc02")
	fmt.Println(string(value))
	_, value = app.get("kljuc03")
	fmt.Println(string(value))
	_, value = app.get("kljuc04")
	fmt.Println(string(value))
	_, value = app.get("kljuc05")
	fmt.Println(string(value))
	_, value = app.get("kljuc06")
	fmt.Println(string(value))
	_, value = app.get("kljuc07")
	fmt.Println(string(value))
	_, value = app.get("kljuc08")
	fmt.Println(string(value))
	_, value = app.get("kljuc09")
	fmt.Println(string(value))
	_, value = app.get("kljuc10")
	fmt.Println(string(value))
	_, value = app.get("kljuc11")
	fmt.Println(string(value))
	_, value = app.get("kljuc12")
	fmt.Println(string(value))
	_, value = app.get("kljuc13")
	fmt.Println(string(value))
	_, value = app.get("kljuc14")
	fmt.Println(string(value))
	_, value = app.get("kljuc15")
	fmt.Println(string(value))
	_, value = app.get("kljuc16")
	fmt.Println(string(value))
	_, value = app.get("kljuc17")
	fmt.Println(string(value))
	_, value = app.get("kljuc18")
	fmt.Println(string(value))
	_, value = app.get("kljuc19")
	fmt.Println(string(value))
	_, value = app.get("kljuc20")
	fmt.Println(string(value))
	_, value = app.get("kljuc21")
	fmt.Println(string(value))
	_, value = app.get("kljuc22")
	fmt.Println(string(value))
	_, value = app.get("kljuc23")
	fmt.Println(string(value))
	_, value = app.get("kljuc24")
	fmt.Println(string(value))
	_, value = app.get("kljuc25")
	fmt.Println(string(value))
	_, value = app.get("kljuc26")
	fmt.Println(string(value))
	_, value = app.get("kljuc27")
	fmt.Println(string(value))
	_, value = app.get("kljuc28")
	fmt.Println(string(value))
	_, value = app.get("kljuc29")
	fmt.Println(string(value))
	_, value = app.get("kljuc30")
	fmt.Println(string(value))
	_, value = app.get("kljuc31")
	fmt.Println(string(value))
	_, value = app.get("kljuc32")
	fmt.Println(string(value))
	_, value = app.get("kljuc33")
	fmt.Println(string(value))
	_, value = app.get("kljuc34")
	fmt.Println(string(value))
	_, value = app.get("kljuc35")
	fmt.Println(string(value))
	_, value = app.get("kljuc36")
	fmt.Println(string(value))
	_, value = app.get("kljuc37")
	fmt.Println(string(value))

	return true
}
