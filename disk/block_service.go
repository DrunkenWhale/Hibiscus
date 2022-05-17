package disk

import (
	"bufio"
	"os"
	"strconv"
)

const (
	blockSize            = 4096
	leafNodeBlockMaxSize = 30

	byteSep       = 3
	byteSepString = string(rune(byteSep))

	leafNodeDataStoragePrefix  = "leaf_"
	leakNodeDataStoragePrefix  = "free_"
	indexNodeDataStoragePrefix = "index_"
)

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
	return NewLeafBlock(int64(id_),
		int64(maxKey),
		int64(nextBlockID),
		int64(kvsSize),
		kvs), nil
}

func WriteLeafBlockFromDiskByBlockID(leaf *LeafBlock, tableName string) error {
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
	leaf.nextBlockID = newBlockID
	bound := leaf.kvsSize / 2
	newLeafKVS := make([]*KV, leafNodeBlockMaxSize+1)
	for i := bound; i < leaf.kvsSize; i++ {
		newLeafKVS[i-bound] = leaf.kvs[i]
		leaf.kvs[i] = nil
	}
	newLeaf.maxKey = leaf.maxKey
	leaf.maxKey = leaf.kvs[bound-1].Key
	newLeaf.kvs = newLeafKVS
	newLeaf.kvsSize = leaf.kvsSize - bound
	leaf.kvsSize = bound
	return leaf, newLeaf
}

func NextLeafNodeBlockID(tableName string) int64 {
	num := nextLeakDataBlockID(tableName)
	if num == -1 {
		return nextLeafDataOrderBlockID(tableName)
	} else {
		return num
	}
}

func nextLeafDataOrderBlockID(tableName string) int64 {
	stat, err := os.Stat(leafNodeDataStoragePrefix + tableName)
	if err != nil {
		panic(err)
	}
	nextBlockID_ := stat.Size() / blockSize
	return nextBlockID_
}

func nextLeakDataBlockID(tableName string) int64 {
	file, err := os.OpenFile(leakNodeDataStoragePrefix+tableName, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	stat, err := file.Stat()
	if err != nil {
		panic(stat)
	}
	if stat.Size() < 1 {
		return -1
	}
	bytes := make([]byte, 8)
	_, err = file.ReadAt(bytes, stat.Size()-8)
	if err != nil {
		panic(err)
	}
	err = file.Truncate(stat.Size() - 8)
	if err != nil {
		panic(err)
	}

	num, err := strconv.ParseInt(string(bytes), 16, 64)
	if err != nil {
		panic(err)
	}
	return num
}
