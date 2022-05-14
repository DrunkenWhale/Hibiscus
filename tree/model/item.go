package model

type BPlusTreeItem struct {
	Key int64
	Val []byte
}

func NewBPlusTreeItem(key int64, val []byte) *BPlusTreeItem {
	return &BPlusTreeItem{
		Key: key,
		Val: val,
	}
}
