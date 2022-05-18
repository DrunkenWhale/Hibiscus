package test

import (
	"Hibiscus/disk"
	"strconv"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	_ = disk.NewBPTree("test")
}

func TestBPTree_Insert(t *testing.T) {
	tree := disk.NewBPTree("test")
	tree.Insert(114, []byte("514"))
	for i := 1; i < 2700; i++ {
		tree.Insert(int64(i+114), []byte(strconv.Itoa(i)))
	}
}
