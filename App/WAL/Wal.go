package WAL

import (
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WAL struct {
	segmentSize    uint8
	currentSegment uint8
	lwm            uint8
	file           *os.File
	mainMap        map[string][]byte
}

func (wal *WAL) SetFile(f *os.File) {
	wal.file = f
}

func (wal *WAL) SetSegmentSize(size uint8) {
	wal.segmentSize = size
}

func (wal *WAL) SetLWM(lwm uint8) {
	wal.lwm = lwm
}

func (wal *WAL) SetMainMap(m map[string][]byte) {
	wal.mainMap = m
}

func (wal *WAL) GetFileName() string {
	return wal.file.Name()
}

func (wal *WAL) GetLMW() uint8 {
	return wal.lwm
}

func CreateWAL(segmensSize, lwm uint8) *WAL {
	files, _ := ioutil.ReadDir("Data/wal/segments")
	maxName := 0
	for _, f := range files {
		str := f.Name()
		x := strings.Split(str, ".bin")
		x = strings.Split(x[0], "wal_")
		num, _ := strconv.Atoi(x[1])
		if num > maxName {
			maxName = num
		}
	}
	wal := WAL{}
	wal.segmentSize = segmensSize
	wal.lwm = lwm
	wal.mainMap = make(map[string][]byte)
	if maxName != 0 {
		wal.file, _ = os.OpenFile("Data/wal/segments/wal_"+strconv.Itoa(maxName)+".bin", os.O_RDWR, 0777)
	} else {
		wal.file, _ = os.Create("Data/wal/segments/wal_1.bin")
	}

	return &wal

}

func (w *WAL) AddElement(key string, value []byte) bool {
	file := w.file
	crc := CRC32(value)
	crc_final := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc_final, crc)

	now := time.Now()
	timestamp := now.Unix()
	timestamp_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp_final, uint64(timestamp))

	tombstone_final := make([]byte, 1)

	var keySize uint64 = uint64(len([]byte(key)))
	keySize_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize_final, keySize)

	var valueSize uint64 = uint64(len(value))
	valueSize_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize_final, valueSize)

	temp := make([]byte, 0)

	temp = append(temp, crc_final...)
	temp = append(temp, timestamp_final...)
	temp = append(temp, tombstone_final...)
	temp = append(temp, keySize_final...)
	temp = append(temp, valueSize_final...)
	temp = append(temp, []byte(key)...)
	temp = append(temp, value...)

	err := appendWal(file, temp)
	if err != nil {
		return false
	}
	w.currentSegment += 1
	if w.currentSegment >= w.segmentSize {
		w.currentSegment = 0
		str := w.file.Name()
		x := strings.Split(str, ".bin")
		x = strings.Split(x[0], "wal_")
		num, _ := strconv.Atoi(x[1])
		err := w.file.Close()
		if err != nil {
			return false
		}
		w.file, err = os.Create("Data/wal/segments/wal_" + strconv.Itoa(num+1) + ".bin")
		if err != nil {
			return false
		}
	}

	w.mainMap[key] = value
	return true
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func appendWal(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	mmapf.Unmap()
	return nil
}

func (w *WAL) DeleteElement(key string, value []byte) bool {
	file := w.file
	crc := CRC32(value)
	crc_final := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc_final, crc)

	now := time.Now()
	timestamp := now.Unix()
	timestamp_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp_final, uint64(timestamp))

	tombstone_final := []byte{1}

	var keySize uint64 = uint64(len([]byte(key)))
	keySize_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize_final, keySize)

	var valueSize uint64 = uint64(len(value))
	valueSize_final := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize_final, valueSize)

	temp := make([]byte, 0)

	temp = append(temp, crc_final...)
	temp = append(temp, timestamp_final...)
	temp = append(temp, tombstone_final...)
	temp = append(temp, keySize_final...)
	temp = append(temp, valueSize_final...)
	temp = append(temp, []byte(key)...)
	temp = append(temp, value...)

	err := appendWal(file, temp)
	if err != nil {
		return false
	}
	w.currentSegment += 1
	if w.currentSegment >= w.segmentSize {
		w.currentSegment = 0
		str := w.file.Name()
		x := strings.Split(str, ".bin")
		x = strings.Split(x[0], "wal_")
		num, _ := strconv.Atoi(x[1])
		err := w.file.Close()
		if err != nil {
			return false
		}
		w.file, _ = os.Create("Data/wal/segments/wal_" + strconv.Itoa(num+1) + ".bin")
	}

	delete(w.mainMap, key)

	return true
}

func (w *WAL) DeleteSegments() {
	for i := 1; uint8(i) < w.lwm; i++ {
		err := os.Remove("Data/wal/segments/wal_" + strconv.Itoa(i) + ".bin")
		if err != nil {
			panic(err)
		}
	}
	other, _ := ioutil.ReadDir("Data/wal/segments")
	for j, f := range other {
		str := f.Name()
		err := os.Rename("Data/wal/segments/"+str, "Data/wal/segments/wal_"+strconv.Itoa(j+1)+".bin")
		if err != nil {
			panic(err)
		}
	}
}

func (w *WAL) ResetWAL() *WAL {
	w.file.Close()
	w.file.Sync()
	err := os.RemoveAll("Data/wal/segments/")
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll("Data/wal/segments/", 0777)
	if err != nil {
		panic(err)
	}
	nw := CreateWAL(5, 3)
	return nw
}

func (w *WAL) Finish() {
	w.file.Close()
}
