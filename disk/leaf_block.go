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

// ToBytes
// return
// convert leaf to bytes slice
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

// Put
// return :
// false => key exist
// true  => operation succeed
func (leaf *LeafBlock) Put(key int64, value []byte) bool {
	if len(leaf.kvs) == 0 {
		leaf.kvs = append(leaf.kvs, NewKV(key, value))
		leaf.kvsSize++
		leaf.maxKey = key
		return true
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.kvs[mid].Key == key {
			return false // key exist
		} else if leaf.kvs[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	index := left
	leaf.kvs = append(leaf.kvs, nil)
	copy(leaf.kvs[index+1:], leaf.kvs[index:])
	leaf.kvs[index] = NewKV(key, value)
	if key > leaf.maxKey {
		leaf.maxKey = key
	}
	leaf.kvsSize++
	return true
}

// Get
// return :
// true  ,data => query data succeed, return data in second return data
// false ,nil  => query data failed, key not existed
func (leaf *LeafBlock) Get(key int64) (bool, []byte) {
	if len(leaf.kvs) == 0 {
		return false, nil
	}
	if key < leaf.kvs[0].Key || key > leaf.maxKey {
		return false, nil
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.kvs[mid].Key == key {
			return true, leaf.kvs[mid].Value
		} else if leaf.kvs[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if leaf.kvs[left].Key == key {
		return true, leaf.kvs[left].Value
	}
	return false, nil
}

//Update
//return :
// false => Update failed, key unexist
// ture  => Update succeed
func (leaf *LeafBlock) Update(key int64, value []byte) bool {
	if len(leaf.kvs) == 0 {
		return false
	}
	if key < leaf.kvs[0].Key || key > leaf.maxKey {
		return false
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.kvs[mid].Key == key {
			leaf.kvs[mid].Value = value
			return true
		} else if leaf.kvs[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if leaf.kvs[left].Key == key {
		leaf.kvs[left].Value = value
		return true
	}
	return false
}
