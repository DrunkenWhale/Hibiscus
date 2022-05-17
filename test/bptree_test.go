package disk

import (
	"strconv"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	_ = NewBPTree("test")
}

func TestBPTree_Insert(t *testing.T) {
	tree := NewBPTree("test")
	for i := 0; i < 27; i++ {
		err := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
	}
}
