package TokenBucket

import (
	"time"
)

type TokenBucket struct {
	tokensMax     int
	tokensLeft    int
	lastReset     int64
	resetInterval int64
}

func CreateTokenBucket(size, resetInterval int) *TokenBucket {
	now := time.Now()
	timestamp := now.Unix()
	tb := TokenBucket{size, size, timestamp, int64(resetInterval)}
	return &tb
}

func (token *TokenBucket) Update() bool {
	now := time.Now()
	timestamp := now.Unix()

	//time.Sleep(2 * time.Second)
	//fmt.Println(timestamp - token.lastReset)

	if timestamp-token.lastReset > token.resetInterval {
		token.tokensLeft = token.tokensMax
		token.lastReset = timestamp
	}
	if token.tokensLeft <= 0 {
		return false
	}
	token.tokensLeft -= 1
	return true
}
