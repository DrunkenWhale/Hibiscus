package model

type BPlusTreeNode struct {
	MaxKey int64
	Nodes  []*BPlusTreeNode
	Items  []BPlusTreeItem
	Next   *BPlusTreeNode
}

func NewBPlusTreeNode(width int) *BPlusTreeNode {
	return &BPlusTreeNode{
		Items: make([]BPlusTreeItem, width+1)[0:0],
	}
}
