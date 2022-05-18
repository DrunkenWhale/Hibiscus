package test

import (
	"Hibiscus/disk"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	_ = disk.NewBPTree("test")
}

func TestBPTree_Insert(t *testing.T) {
	tree := disk.NewBPTree("test")
	tree.Insert(114, []byte("514"))
	for _, i := range rand.Perm(5000) {
		ok := tree.Insert(int64(i+617), []byte(strconv.Itoa(i)))
		if !ok {
			fmt.Println(i)
		}
	}
	return
}
