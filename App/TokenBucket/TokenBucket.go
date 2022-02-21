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

func CreateTokenBucket(size, resetInterval, tokensLeft int, lastReset int64) *TokenBucket {
	token := TokenBucket{size, tokensLeft, lastReset, int64(resetInterval)}
	now := time.Now()
	timestamp := now.Unix()
	if timestamp-token.lastReset > token.resetInterval {
		token.tokensLeft = token.tokensMax
		token.lastReset = timestamp
	}
	return &token
}

func (token *TokenBucket) GetTokensLeft() int {
	return token.tokensLeft
}

func (token *TokenBucket) GetLastReset() int64 {
	return token.lastReset
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
