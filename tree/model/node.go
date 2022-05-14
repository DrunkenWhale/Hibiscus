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

func (node *BPlusTreeNode) insertValue(key int64, value []byte) {
	item := NewBPlusTreeItem(key, value)
	num := len(node.Items)
	if num < 1 {
		node.Items = append(node.Items, item)
		node.MaxKey = item.Key
		return
	} else if key < node.Items[0].Key {
		node.Items = append([]BPlusTreeItem{item}, node.Items...)
		return
	} else if key > node.Items[num-1].Key {
		node.Items = append(node.Items, item)
		node.MaxKey = item.Key
		return
	}

	for i := 0; i < num; i++ {
		// division
		if node.Items[i].Key > key {
			node.Items = append(node.Items, BPlusTreeItem{})
			copy(node.Items[i+1:], node.Items[i:])
			node.Items[i] = item
			return
		} else if node.Items[i].Key == key {
			node.Items[i] = item
			return
		}
	}
	return
}
