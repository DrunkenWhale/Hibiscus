package model

type BPlusTreeNode struct {
	// the max key in this node
	// for example
	// [114, 241, 985, 514]
	// MaxKey = 514
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
