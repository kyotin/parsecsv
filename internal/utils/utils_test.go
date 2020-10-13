package utils

import (
	"fmt"
	"testing"
)

func TestHashSHA1(t *testing.T) {
	str1 := "adafadsfasfasdf"
	str2 :=  "adafadsfasfasdf"
	h1 := HashSHA1(str1)
	h2 := HashSHA1(str2)

	fmt.Println(h1)
	fmt.Println(h2)
}