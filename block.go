package main

import "strconv"

const (
	blockSize   = 4096
	maxLeafSize = 30
)

type IndexBlock struct {
	id      uint64
	keys    []int64
	blockID []uint64
}

type LeafBlock struct {
	// block id
	id          uint64
	nextBlockID uint64
	prevBlockID uint64
	kvs         []*KV
}

func (leaf *LeafBlock) ToBytes() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, []byte(strconv.FormatUint(leaf.id, 10)+" "+
		strconv.FormatUint(leaf.prevBlockID, 10)+" "+
		strconv.FormatUint(leaf.nextBlockID, 10)+" ")...,
	)
	for _, kv := range leaf.kvs {
		bytes = append(bytes, kv.ToBytes()...)
	}
	return bytes
}

type LeafLeakBlock struct {
	blockID uint64
}
