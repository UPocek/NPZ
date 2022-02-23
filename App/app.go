package App

import (
	"Projekat/App/BloomFilter"
	"Projekat/App/CMS"
	"Projekat/App/Cache"
	"Projekat/App/HLL"
	"Projekat/App/Memtable"
	"Projekat/App/TokenBucket"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type User struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	tokenBucket *TokenBucket.TokenBucket
}

type App struct {
	memtable    *Memtable.Memtable
	cache       *Cache.Cache
	tokenBucket *TokenBucket.TokenBucket
	data        map[string]int
	user        User
	cms         map[string]string
	hll         map[string]string
}

func CreateApp() App {
	app := App{}
	app.data = make(map[string]int)
	fromYaml := false
	yfile, err := ioutil.ReadFile("config.yaml")
	if err == nil {
		yaml.Unmarshal(yfile, &app.data)
		fromYaml = true
	} else {
		app.data["wal_size"] = 5
		app.data["wal_lwm"] = 3
		app.data["memtable_size"] = 10
		app.data["memtable_threshold"] = 70
		app.data["cache_max_size"] = 5
		app.data["lsm_max_lvl"] = 3
		app.data["lsm_merge_threshold"] = 2
		app.data["skiplist_max_height"] = 10
		app.data["hll_precision"] = 4
		app.data["tokenbucket_size"] = 200
		app.data["tokenbucket_interval"] = 60
		app.data["cmsEpsilon"] = 1
		app.data["cmsDelta"] = 1
	}
	app.memtable = Memtable.CreateMemtable(app.data, fromYaml)
	app.cache = Cache.CreateCache(uint32(app.data["cache_max_size"]))
	app.hll = make(map[string]string)
	app.cms = make(map[string]string)
	app.createNewHLL("default", uint8(app.data["hll_precision"]))
	app.createNewCMS("default", float64(app.data["cmsEpsilon"])*0.01, float64(app.data["cmsDelta"])*0.01)

	return app
}

func (app *App) RunApp(amateur bool) {
	if !amateur {
		http.HandleFunc("/login/", app.login)
		http.HandleFunc("/data/", app.users)
		http.HandleFunc("/", app.index)

		port := os.Getenv("PORT")
		if port == "" {
			port = "9000"
		}
		err := http.ListenAndServe(":"+port, nil)
		if err != nil {
			panic(err)
		}
	} else {
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

				if app._login(username, password) {
					app.options()
				}
			} else if strings.Replace(option, ")", "", 1) == "2" {
				fmt.Print("Unesite korisničko ime\n >> ")
				var username string
				fmt.Scanln(&username)

				fmt.Print("Unesite lozinku\n >> ")
				var password string
				fmt.Scanln(&password)

				if !app._login(username, password) {
					file, err := os.OpenFile("Data/users/users.csv", os.O_APPEND|os.O_CREATE, 0777)
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
}

func (app *App) options() {
	for true {
		fmt.Print("Izaberite jednu od opcija:\n 1) Put \n 2) Get \n 3) Delete \n 4) PutSpecial \n 5) Test \n 6) Logout \n >> ")
		var option string
		fmt.Scanln(&option)
		if strings.Replace(option, ")", "", 1) == "1" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			ok := app._put(key, []byte(value))
			if ok {
				fmt.Println("\nDodavanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nGreška: Prilikom dodavanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "2" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			ok, value := app._get(key)
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
			ok := app._delete(key, []byte(value))
			if ok {
				fmt.Println("\nBrisanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nGreška: Prilikom brisanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "4" {
			fmt.Print("Unesite ključ\n >> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Unesite vrednost\n >> ")
			var value string
			fmt.Scanln(&value)
			fmt.Print("Unesite kategoriju\n >> ")
			var whichOne string
			fmt.Scanln(&whichOne)
			ok := app._putSpecial(key, []byte(value), whichOne)
			if ok {
				fmt.Println("\nDodavanje elementa je uspešno odradjeno!\n")
			} else {
				fmt.Println("\nGreška: Prilikom dodavanja elementa!\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "5" {
			ok := app.Test()
			if !ok {
				fmt.Println("\nGreška: Prilikom testiranja\n")
			}
		} else if strings.Replace(option, ")", "", 1) == "6" {
			break
		} else {
			fmt.Println("\nGreška: Uneta opcija ne postoji pokušajte ponovo.\n")
		}
		app.updateUsersFile()
	}
}

func (app *App) StopApp() {
	app.memtable.Finish()
	os.Exit(0)
}

func (app *App) createNewHLL(key string, precision uint8) bool {
	_, ok := app.hll[key]
	if ok {
		fmt.Println("HLL pod datim ključem već postoji!")
		return false
	}

	newHLL := HLL.CreateHLL(precision)
	name := newHLL.SerializeHLL(key)
	app.hll[key] = name

	return true
}

func (app *App) createNewCMS(key string, epsilon, delta float64) bool {
	_, ok := app.cms[key]
	if ok {
		fmt.Println("CMS pod datim ključem već postoji!")
		return false
	}

	newCMS := CMS.CreateCountMinSketch(epsilon, delta)
	name := newCMS.SerializeCountMinSketch(key)
	app.cms[key] = name
	return true
}

func (app *App) addToSpecialHLL(key string, whichOne string) bool {
	name, ok := app.hll[whichOne]
	if !ok {
		app.createNewHLL(whichOne, uint8(app.data["hll_precision"]))
	}
	name, _ = app.hll[whichOne]
	sHLL := HLL.DeserializeHLL(name)
	sHLL.AddElement(key)
	sHLL.SerializeHLL(whichOne)
	return true
}

func (app *App) addToSpecialCMS(key string, whichOne string) bool {
	name, ok := app.cms[whichOne]
	if !ok {
		app.createNewCMS(whichOne, float64(app.data["cmsEpsilon"])*0.01, float64(app.data["cmsDelta"])*0.01)
	}
	name, _ = app.cms[whichOne]
	sCMS := CMS.DeserializeCountMinSketch(name)
	sCMS.AddElement(key)
	sCMS.SerializeCountMinSketch(whichOne)
	return true
}

func (app *App) getEstimateFromSpecialHLL(whichOne string) float64 {
	sHLL := HLL.DeserializeHLL(app.hll[whichOne])
	return sHLL.Estimate()
}

func (app *App) getFrequencyFromSpecialCMS(key string, whichOne string) uint {
	sCMS := CMS.DeserializeCountMinSketch(app.cms[whichOne])
	return sCMS.FrequencyOfElement(key)
}

func (app *App) _putSpecial(key string, value []byte, whichOne string) bool {
	if app.tokenBucket.Update() {
		ok := app._put(key, value)
		if ok {
			app.addToSpecialHLL(key, whichOne)
			app.addToSpecialCMS(key, whichOne)
			return true
		}
		return false
	}
	fmt.Println("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	jsonBytes, err := json.Marshal("")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonBytes)
}

func (app *App) login(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == "GET" {
		tokens := strings.Split(r.URL.String(), "/")
		if len(tokens) != 3 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		info := strings.Split(tokens[2], ",")
		if len(info) != 2 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		ok := app._login(info[0], info[1])

		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		app.updateUsersFile()
		return
	} else {
		tokens := strings.Split(r.URL.String(), "/")
		if len(tokens) != 3 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		info := strings.Split(tokens[2], ",")
		if len(info) != 2 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !app._login(info[0], info[1]) {
			file, err := os.OpenFile("Data/users/users.csv", os.O_APPEND|os.O_WRONLY, 0777)
			if err != nil {
				panic(err)
			}
			_, err = file.WriteString(info[0] + "," + info[1] + "," + strconv.Itoa(app.data["tokenbucket_size"]) + "," + strconv.Itoa(int(time.Now().Unix())) + "\n")
			if err != nil {
				panic(err)
			}
			file.Close()
			w.WriteHeader(http.StatusOK)
			return
		} else {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
}

func (app *App) get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	tokens := strings.Split(r.URL.String(), "/")
	if len(tokens) != 3 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	ok, value := app._get(tokens[2])

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(value)
}

func (app *App) put(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	tokens := strings.Split(r.URL.String(), "/")
	if len(tokens) != 3 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	info := strings.Split(tokens[2], ",")
	if len(info) != 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	ok := app._put(info[0], []byte(info[1]))
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	tokens := strings.Split(r.URL.String(), "/")
	if len(tokens) != 3 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	info := strings.Split(tokens[2], ",")
	if len(info) != 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	ok := app._delete(info[0], []byte(info[1]))
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) users(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	switch r.Method {
	case "GET":
		app.get(w, r)
		app.updateUsersFile()
		return
	case "POST", "PUT":
		app.put(w, r)
		app.updateUsersFile()
		return
	case "OPTIONS":
		app.delete(w, r)
		app.updateUsersFile()
		return
	default:
		return
	}
}

func (app *App) _put(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		ok := app.memtable.Write(key, value)
		if ok {
			cms := CMS.DeserializeCountMinSketch(app.cms["default"])
			cms.AddElement(key)
			cms.SerializeCountMinSketch("default")
			hll := HLL.DeserializeHLL(app.hll["default"])
			hll.AddElement(key)
			hll.SerializeHLL("default")
		}
		return ok
	}
	fmt.Println("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) _get(key string) (bool, []byte) {
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

func (app *App) _delete(key string, value []byte) bool {
	if app.tokenBucket.Update() {
		answer, _ := app._get(key)
		app.cache.RemoveElement(key)
		return app.memtable.Delete(key, value, answer)
	}
	fmt.Println("Greška: Dostigli ste maksimalan broj zahteva. Pokušajte ponovo kasnije")
	return false
}

func (app *App) _login(username, password string) bool {
	file, err := os.OpenFile("Data/users/users.csv", os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		items := strings.Split(scanner.Text(), ",")
		if username == items[0] && password == items[1] {
			app.user.Username = username
			app.user.Password = password
			tokensLeft, _ := strconv.Atoi(items[2])
			lastReset, _ := strconv.Atoi(items[3])
			app.user.tokenBucket = TokenBucket.CreateTokenBucket(app.data["tokenbucket_size"], app.data["tokenbucket_interval"], tokensLeft, int64(lastReset))
			app.tokenBucket = app.user.tokenBucket
			return true
		}
	}
	return false
}

func (app *App) updateUsersFile() {
	file, err := os.OpenFile("Data/users/users.csv", os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	list := make([]byte, 0)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), app.user.Username) {
			list = append(list, app.user.Username+","+app.user.Password+","+strconv.Itoa(app.user.tokenBucket.GetTokensLeft())+","+strconv.Itoa(int(app.user.tokenBucket.GetLastReset()))+"\n"...)
		} else {
			list = append(list, []byte(scanner.Text()+"\n")...)
		}
	}
	file.Close()

	file, err = os.OpenFile("Data/users/users.csv", os.O_RDWR, 0777)
	file.WriteString(string(list))
	file.Close()

}

func (app *App) Test() bool {
	app._put("kljuc01", []byte("vrednost1"))
	app._put("kljuc02", []byte("vrednost2"))
	app._put("kljuc03", []byte("vrednost3"))
	app._put("kljuc04", []byte("vrednost4"))
	app._put("kljuc05", []byte("vrednost5"))
	app._put("kljuc06", []byte("vrednost6"))
	app._put("kljuc07", []byte("vrednost7"))

	app._put("kljuc08", []byte("vrednost8"))
	app._delete("kljuc03", []byte("vrednost3"))
	app._put("kljuc10", []byte("vrednost10"))
	app._put("kljuc11", []byte("vrednost11"))
	app._put("kljuc12", []byte("vrednost12"))
	app._put("kljuc13", []byte("vrednost13"))
	app._put("kljuc14", []byte("vrednost14"))

	app._delete("kljuc15", []byte("vrednost15"))
	app._delete("kljuc05", []byte("vrednost5"))
	app._delete("kljuc06", []byte("vrednost6"))
	app._put("kljuc18", []byte("vrednost18"))
	app._delete("kljuc08", []byte("vrednost08"))
	app._put("kljuc20", []byte("vrednost20"))
	app._put("kljuc21", []byte("vrednost21"))
	app._put("kljuc30", []byte("vrednost30"))

	app._put("kljuc23", []byte("vrednost23"))
	app._put("kljuc24", []byte("vrednost24"))
	app._put("kljuc25", []byte("vrednost25"))
	app._put("kljuc26", []byte("vrednost26"))
	app._put("kljuc27", []byte("vrednost27"))
	app._put("kljuc01", []byte("vrednost01"))
	app._put("kljuc28", []byte("vrednost28"))

	app._put("kljuc31", []byte("vrednost31"))
	app._put("kljuc32", []byte("vrednost32"))
	app._put("kljuc33", []byte("vrednost33"))
	app._put("kljuc34", []byte("vrednost34"))
	app._put("kljuc35", []byte("vrednost35"))
	app._put("kljuc36", []byte("vrednost36"))
	app._put("kljuc37", []byte("vrednost37"))

	app._put("kljuc08", []byte("vrednost8"))
	app._delete("kljuc07", []byte("vrednost7"))
	app._put("kljuc10", []byte("vrednost10"))
	app._put("kljuc11", []byte("vrednost11"))
	app._put("kljuc12", []byte("vrednost12"))
	app._put("kljuc13", []byte("vrednost13"))
	app._put("kljuc14", []byte("vrednost14"))

	app._delete("kljuc15", []byte("vrednost15"))
	app._delete("kljuc05", []byte("vrednost5"))
	app._delete("kljuc06", []byte("vrednost6"))
	app._put("kljuc18", []byte("vrednost18"))
	app._delete("kljuc08", []byte("vrednost08"))
	app._put("kljuc20", []byte("vrednost20"))
	app._put("kljuc21", []byte("vrednost21"))
	app._put("kljuc30", []byte("vrednost30"))

	app._put("kljuc23", []byte("vrednost23"))
	app._put("kljuc24", []byte("vrednost24"))
	app._put("kljuc25", []byte("vrednost25"))
	app._put("kljuc26", []byte("vrednost26"))
	app._put("kljuc27", []byte("vrednost27"))
	app._put("kljuc01", []byte("vrednost01"))
	app._put("kljuc28", []byte("vrednost28"))

	app._put("kljuc01", []byte("vrednost1"))
	app._put("kljuc02", []byte("nova_vrednost2"))
	app._put("kljuc03", []byte("vrednost3"))
	app._put("kljuc04", []byte("vrednost4"))
	app._put("kljuc05", []byte("vrednost5"))
	app._put("kljuc06", []byte("vrednost6"))
	app._put("kljuc07", []byte("vrednost7"))

	app._put("kljuc08", []byte("vrednost8"))
	app._delete("kljuc03", []byte("vrednost3"))
	app._put("kljuc10", []byte("vrednost10"))
	app._put("kljuc11", []byte("vrednost11"))
	app._put("kljuc12", []byte("vrednost12"))
	app._put("kljuc13", []byte("vrednost13"))
	app._put("kljuc14", []byte("vrednost14"))

	app._delete("kljuc15", []byte("vrednost15"))
	app._delete("kljuc05", []byte("vrednost5"))
	app._delete("kljuc06", []byte("vrednost6"))
	app._put("kljuc18", []byte("vrednost18"))
	app._delete("kljuc08", []byte("vrednost08"))
	app._put("kljuc20", []byte("vrednost20"))
	app._put("kljuc21", []byte("vrednost21"))
	app._put("kljuc30", []byte("vrednost30"))

	app._put("kljuc23", []byte("vrednost23"))
	app._put("kljuc24", []byte("vrednost24"))
	app._put("kljuc25", []byte("vrednost25"))
	app._put("kljuc26", []byte("vrednost26"))
	app._put("kljuc27", []byte("vrednost27"))
	app._put("kljuc01", []byte("vrednost01"))
	app._put("kljuc28", []byte("vrednost28"))

	_, value := app._get("kljuc01")
	fmt.Println(string(value))
	_, value = app._get("kljuc02")
	fmt.Println(string(value))
	_, value = app._get("kljuc03")
	fmt.Println(string(value))
	_, value = app._get("kljuc04")
	fmt.Println(string(value))
	_, value = app._get("kljuc05")
	fmt.Println(string(value))
	_, value = app._get("kljuc06")
	fmt.Println(string(value))
	_, value = app._get("kljuc07")
	fmt.Println(string(value))
	_, value = app._get("kljuc08")
	fmt.Println(string(value))
	_, value = app._get("kljuc09")
	fmt.Println(string(value))
	_, value = app._get("kljuc10")
	fmt.Println(string(value))
	_, value = app._get("kljuc11")
	fmt.Println(string(value))
	_, value = app._get("kljuc12")
	fmt.Println(string(value))
	_, value = app._get("kljuc13")
	fmt.Println(string(value))
	_, value = app._get("kljuc14")
	fmt.Println(string(value))
	_, value = app._get("kljuc15")
	fmt.Println(string(value))
	_, value = app._get("kljuc16")
	fmt.Println(string(value))
	_, value = app._get("kljuc17")
	fmt.Println(string(value))
	_, value = app._get("kljuc18")
	fmt.Println(string(value))
	_, value = app._get("kljuc19")
	fmt.Println(string(value))
	_, value = app._get("kljuc20")
	fmt.Println(string(value))
	_, value = app._get("kljuc21")
	fmt.Println(string(value))
	_, value = app._get("kljuc22")
	fmt.Println(string(value))
	_, value = app._get("kljuc23")
	fmt.Println(string(value))
	_, value = app._get("kljuc24")
	fmt.Println(string(value))
	_, value = app._get("kljuc25")
	fmt.Println(string(value))
	_, value = app._get("kljuc26")
	fmt.Println(string(value))
	_, value = app._get("kljuc27")
	fmt.Println(string(value))
	_, value = app._get("kljuc28")
	fmt.Println(string(value))
	_, value = app._get("kljuc29")
	fmt.Println(string(value))
	_, value = app._get("kljuc30")
	fmt.Println(string(value))
	_, value = app._get("kljuc31")
	fmt.Println(string(value))
	_, value = app._get("kljuc32")
	fmt.Println(string(value))
	_, value = app._get("kljuc33")
	fmt.Println(string(value))
	_, value = app._get("kljuc34")
	fmt.Println(string(value))
	_, value = app._get("kljuc35")
	fmt.Println(string(value))
	_, value = app._get("kljuc36")
	fmt.Println(string(value))
	_, value = app._get("kljuc37")
	fmt.Println(string(value))

	app._putSpecial("tasa", []byte("tasa"), "new")

	return true
}
