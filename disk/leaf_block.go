package disk

import (
	"strconv"
)

type LeafBlock struct {
	// block id
	id          int64
	maxKey      int64
	nextBlockID int64
	kvsSize     int64
	kvs         []*KV
}

func NewLeafBlock(
	id int64,
	maxKey int64,
	nextBlockID int64,
	kvsSize int64,
	kvs []*KV,
) *LeafBlock {
	return &LeafBlock{
		id:          id,
		maxKey:      maxKey,
		nextBlockID: nextBlockID,
		kvsSize:     kvsSize,
		kvs:         kvs,
	}
}

func (leaf *LeafBlock) ToBytes() []byte {
	if len(leaf.kvs) > blockSize {
		panic("Too much Key Value Pair")
	}
	bytes := make([]byte, blockSize)
	bytes = bytes[0:0]
	bytes = append(bytes, []byte(strconv.FormatInt(leaf.id, 10)+byteSepString+
		strconv.FormatInt(leaf.maxKey, 10)+byteSepString+
		strconv.FormatInt(leaf.nextBlockID, 10)+byteSepString+
		strconv.FormatInt(leaf.kvsSize, 10)+byteSepString,
	)...)
	for _, kv := range leaf.kvs {
		if kv == nil {
			break
		}
		bytes = append(bytes, kv.ToBytes()...)
	}
	if len(bytes) > blockSize {
		panic("Node Size Too Large")
	}
	return bytes[:4096]
}
