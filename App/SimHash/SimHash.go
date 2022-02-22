package SimHash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

type MySimHash struct {
	text        string
	words       map[string]int
	fingerprint []int
}

func (msh *MySimHash) CreateFingerPrint() {

	stopwords := []string{"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"}
	tokens := strings.Split(msh.text, " ")
	for i, token := range tokens {
		for _, s := range stopwords {
			if s == token {
				tokens[i] = "#nil"
			}
		}
	}
	words := make(map[string]int)
	for _, token := range tokens {
		if token != "#nil" {
			words[token] += 1
		}
	}

	msh.words = words

	table := make([][]string, len(msh.words))
	for m := range table {
		table[m] = make([]string, 256)
	}
	i := 0
	for word, _ := range msh.words {
		str := ToBinary(GetMD5Hash(word))
		for j := 0; j < len(str); j++ {
			if string(str[j]) == "0" {
				table[i][j] = "-1"
			} else {
				table[i][j] = "1"
			}
		}
		i++
	}

	i = 0
	fingerPrint := make([]int, 256)
	for _, count := range msh.words {
		for k := 0; k < len(table[i]); k++ {
			n, _ := strconv.Atoi(table[i][k])
			fingerPrint[k] += n * count
		}
		i++
	}

	for e, el := range fingerPrint {
		if el <= 0 {
			fingerPrint[e] = 0
		} else {
			fingerPrint[e] = 1
		}
	}

	msh.fingerprint = fingerPrint

}

func XOR(msh1, msh2 MySimHash) int {

	n := 0
	for i, el := range msh1.fingerprint {
		if (el == 0 && msh2.fingerprint[i] == 1) || (el == 1 && msh2.fingerprint[i] == 0) {
			n += 1
		}
	}

	return n

}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func main() {
	msh1 := MySimHash{"It is a very nice day today", nil, nil}
	msh1.CreateFingerPrint()
	msh2 := MySimHash{"It was a very nice day yesterday", nil, nil}
	msh2.CreateFingerPrint()
	msh3 := MySimHash{"Banana is a healthy fruit", nil, nil}
	msh3.CreateFingerPrint()

	r := XOR(msh1, msh2)
	fmt.Println("Hamming distance for similar sentences is ", r)
	r = XOR(msh1, msh3)
	fmt.Println("Hamming distance for different sentences is ", r)

}
