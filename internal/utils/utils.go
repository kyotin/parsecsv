package utils

import (
	"crypto/sha1"
	"fmt"
	"hash/fnv"
)

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func HashSHA1(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)

	return fmt.Sprintf("%x\n", bs)
}