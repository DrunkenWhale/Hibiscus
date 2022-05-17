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
	for i := 0; i < 27; i++ {
		err := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
	}
}
