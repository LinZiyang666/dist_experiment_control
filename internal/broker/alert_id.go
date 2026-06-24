package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var alertIDFallback atomic.Uint64

func newAlertID(dedupKey string, now time.Time) string {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err == nil {
		return fmt.Sprintf("%s@%d.%s", dedupKey, now.UnixNano(), hex.EncodeToString(suffix[:]))
	}
	return fmt.Sprintf("%s@%d.%x", dedupKey, now.UnixNano(), alertIDFallback.Add(1))
}
