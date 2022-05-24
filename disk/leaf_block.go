package disk

import (
	"bufio"
	"os"
	"strconv"
)

const (
	leafNodeBlockMaxSize      = 30
	leafNodeDataStoragePrefix = storageDirectoryName + string(os.PathSeparator) + "leaf_"
)

type LeafBlock struct {
	// block id
	id          int64
	maxKey      int64
	nextBlockID int64
	parentIndex int64
	kvsSize     int64
	KVs         []*KV
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
		KVs:         kvs,
		parentIndex: -1,
	}
}

// ToBytes
// return
// convert leaf to bytes slice
func (leaf *LeafBlock) ToBytes() []byte {
	if leaf.kvsSize > leafNodeBlockMaxSize {
		panic("Too much Key Value Pair")
	}
	bytes := make([]byte, blockSize)
	bytes = bytes[0:0]
	bytes = append(bytes, []byte(strconv.FormatInt(leaf.id, 10)+byteSepString+
		strconv.FormatInt(leaf.maxKey, 10)+byteSepString+
		strconv.FormatInt(leaf.nextBlockID, 10)+byteSepString+
		strconv.FormatInt(leaf.parentIndex, 10)+byteSepString+
		strconv.FormatInt(leaf.kvsSize, 10)+byteSepString,
	)...)
	count := int64(0)
	for _, kv := range leaf.KVs {
		if kv == nil {
			break
		}
		count++
		bytes = append(bytes, kv.ToBytes()...)
	}
	if count != leaf.kvsSize {
		panic("Unmatch")
	}
	if len(bytes) > blockSize {
		panic("Leaf Node Size Too Large")
	}
	return bytes[:blockSize]
}

// Put
// return :
// false => Key exist
// true  => operation succeed
func (leaf *LeafBlock) Put(key int64, value []byte) bool {
	if len(leaf.KVs) == 0 {
		leaf.KVs = append(leaf.KVs, NewKV(key, value))
		leaf.kvsSize++
		leaf.maxKey = key
		return true
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.KVs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	index := left
	if left == leaf.kvsSize {
		leaf.KVs = append(leaf.KVs, NewKV(key, value))
	} else if leaf.KVs[index].Key == key {
		// key exist
		// update it
		leaf.KVs[index] = NewKV(key, value)
		return true
	} else {
		leaf.KVs = append(leaf.KVs, nil)
		copy(leaf.KVs[index+1:], leaf.KVs[index:])
		leaf.KVs[index] = NewKV(key, value)
	}
	if key > leaf.maxKey {
		leaf.maxKey = key
	}
	leaf.kvsSize++
	return true
}

// Get
// return :
// true  ,data => query data succeed, return data in second return data
// false ,nil  => query data failed, Key not existed
func (leaf *LeafBlock) Get(key int64) (bool, []byte) {
	if len(leaf.KVs) == 0 {
		return false, nil
	}
	if key < leaf.KVs[0].Key || key > leaf.maxKey {
		return false, nil
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.KVs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == leaf.kvsSize {
		return false, nil
	}
	if leaf.KVs[left].Key == key {
		return true, leaf.KVs[left].Value
	}
	return false, nil
}

//Update
//return :
// false => Update failed, Key unexist
// ture  => Update succeed
func (leaf *LeafBlock) Update(key int64, value []byte) bool {
	if len(leaf.KVs) == 0 {
		return false
	}
	if key < leaf.KVs[0].Key || key > leaf.maxKey {
		return false
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.KVs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == leaf.kvsSize {
		return false
	}
	if leaf.KVs[left].Key == key {
		leaf.KVs[left].Value = value
		return true
	}
	return false
}

func (leaf *LeafBlock) Delete(key int64) bool {
	if len(leaf.KVs) == 0 {
		return false
	}
	if key < leaf.KVs[0].Key || key > leaf.maxKey {
		return false
	}
	left := int64(0)
	right := leaf.kvsSize
	for left < right {
		mid := (right + left) >> 1
		if leaf.KVs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == leaf.kvsSize {
		return false
	}
	if leaf.KVs[left].Key == key {
		if leaf.maxKey == key {
			if left == 0 {
				leaf.maxKey = -1
			} else {
				leaf.maxKey = leaf.KVs[left-1].Key
			}
		}
		leaf.kvsSize--
		leaf.KVs = append(leaf.KVs[:left], leaf.KVs[left+1:]...)
		return true
	}
	return false
}

func ReadLeafBlockFromDiskByBlockID(blockID int64, tableName string) (*LeafBlock, error) {
	file, err := os.OpenFile(leafNodeDataStoragePrefix+tableName, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
		return nil, err
	}
	defer file.Close()

	offset := blockSize * blockID

	_, err = file.Seek(offset, 0)
	if err != nil {
		panic(err)
		return nil, err
	}
	buf := bufio.NewReader(file)

	readString, err := buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	id_, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}
	if int64(id_) != (blockID) {
		panic("Illegal Block ID")
		return nil, nil
	}
	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	maxKey, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}

	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	nextBlockID, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}
	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	parentIndex, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}

	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	kvsSize, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}

	kvs := make([]*KV, kvsSize)
	for i := 0; i < kvsSize; i++ {
		readString, err = buf.ReadString(byteSep)
		if err != nil {
			panic(err)
			return nil, err
		}
		key, err := strconv.Atoi(readString[:len(readString)-1])
		if err != nil {
			panic(err)
			return nil, err
		}
		readString, err = buf.ReadString(byteSep)
		if err != nil {
			panic(err)
			return nil, err
		}
		val := []byte(readString[:len(readString)-1])
		kvs[i] = NewKV(int64(key), val)
	}
	leaf := NewLeafBlock(int64(id_),
		int64(maxKey),
		int64(nextBlockID),
		int64(kvsSize),
		kvs)
	leaf.parentIndex = int64(parentIndex)
	return leaf, nil
}

func WriteLeafBlockToDiskByBlockID(leaf *LeafBlock, tableName string) error {
	file, err := os.OpenFile(leafNodeDataStoragePrefix+tableName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
		return err
	}
	defer file.Close()
	offset := leaf.id * blockSize
	_, err = file.WriteAt(leaf.ToBytes(), offset)
	if err != nil {
		panic(err)
		return err
	}
	return nil
}

func SplitLeafNodeBlock(leaf *LeafBlock, tableName string) (*LeafBlock, *LeafBlock) {
	newBlockID := NextLeafNodeBlockID(tableName)
	newLeaf := NewLeafBlock(newBlockID, 0, 0, 0, nil)
	newLeaf.nextBlockID = leaf.nextBlockID
	newLeaf.parentIndex = leaf.parentIndex
	leaf.nextBlockID = newBlockID
	bound := leaf.kvsSize / 2
	newLeafKVS := make([]*KV, leafNodeBlockMaxSize+1)

	for i := bound; i < leaf.kvsSize; i++ {
		newLeafKVS[i-bound] = leaf.KVs[i]
		leaf.KVs[i] = nil
	}
	newLeaf.maxKey = leaf.maxKey
	leaf.maxKey = leaf.KVs[bound-1].Key
	newLeaf.kvsSize = leaf.kvsSize - bound
	leaf.kvsSize = bound
	newLeaf.KVs = newLeafKVS[:newLeaf.kvsSize]
	leaf.KVs = leaf.KVs[:bound]
	return leaf, newLeaf
}

//func NextLeafNodeBlockID(tableName string) int64 {
//	num := nextLeakDataBlockID(tableName)
//	if num == -1 {
//		return nextLeafDataOrderBlockID(tableName)
//	} else {
//		return num
//	}
//}
//
//func nextLeafDataOrderBlockID(tableName string) int64 {
//
//	file, err := os.OpenFile(leakNodeDataStoragePrefix+tableName, os.O_CREATE|os.O_RDWR, 0666)
//	defer file.Close()
//	if err != nil {
//		panic(err)
//	}
//	stat, err := file.Stat()
//	if err != nil {
//		panic(stat)
//	}
//	if stat.Size() < 1 {
//		return -1
//	}
//	bytes := make([]byte, 8)
//	_, err = file.ReadAt(bytes, stat.Size()-8)
//	if err != nil {
//		panic(err)
//	}
//	err = file.Truncate(stat.Size() - 8)
//	if err != nil {
//		panic(err)
//	}
//
//	num, err := strconv.ParseInt(string(bytes), 16, 64)
//	if err != nil {
//		panic(err)
//	}
//	return num
//}
//
//func nextLeakDataBlockID(tableName string) int64 {
//	file, err := os.OpenFile(leakNodeDataStoragePrefix+tableName, os.O_CREATE|os.O_RDWR, 0666)
//	defer file.Close()
//	if err != nil {
//		panic(err)
//	}
//	stat, err := file.Stat()
//	if err != nil {
//		panic(stat)
//	}
//	if stat.Size() < 1 {
//		return -1
//	}
//	bytes := make([]byte, 8)
//	_, err = file.ReadAt(bytes, stat.Size()-8)
//	if err != nil {
//		panic(err)
//	}
//	err = file.Truncate(stat.Size() - 8)
//	if err != nil {
//		panic(err)
//	}
//
//	num, err := strconv.ParseInt(string(bytes), 16, 64)
//	if err != nil {
//		panic(err)
//	}
//	return num
//}
