package util

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func GenerateTxID(prefix, sender, receiver string, amount uint64, timestampNano int64) string {
	h := sha256.New()
	h.Write([]byte(prefix))
	h.Write([]byte(sender))
	h.Write([]byte(receiver))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], amount)
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(timestampNano))
	h.Write(buf[:])
	sum := h.Sum(nil)
	return fmt.Sprintf("%s%x", prefix, sum[:16])
}
