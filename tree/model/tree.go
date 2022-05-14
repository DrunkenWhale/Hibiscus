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

func (tree *BPlusTree) Query(key int64) (bool, []byte) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	node := tree.root
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey {
			node = node.Nodes[i]
			i = 0
		}
	}

	if len(node.Nodes) > 0 {
		return false, nil
	}

	for i := 0; i < len(node.Items); i++ {
		if node.Items[i].Key == key {
			return true, node.Items[i].Val
		}
	}
	return false, nil
}

func (tree *BPlusTree) Insert(key int64) (bool, []byte) {

}

func (tree *BPlusTree) Delete(key int64) (bool, []byte) {

}
