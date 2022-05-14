package model

import "sync"

type BPlusTree struct {
	mutex     sync.RWMutex
	root      *BPlusTreeNode
	width     int
	halfWidth int
}

func NewBPlusTree(width int) *BPlusTree {
	if width < 3 {
		width = 3
	}
	return &BPlusTree{
		root:      NewBPlusTreeNode(width),
		width:     width,
		halfWidth: (width + 1) / 2,
	}
}

func (tree *BPlusTree) Get(key int64) (bool, []byte) {

}
func (tree *BPlusTree) Insert(key int64) (bool, []byte) {

}
func (tree *BPlusTree) Delete(key int64) (bool, []byte) {

}
