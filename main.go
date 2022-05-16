package main

import (
	"fmt"
	"strconv"
)

func main() {
	num := (1 << 63) - 1
	bytes := make([]byte, 16)
	numString := strconv.FormatInt(int64(num), 16)
	offset := 16 - (len(numString))
	bytes = append(bytes[:offset], numString...)
	fmt.Println(string(bytes))
}
