package disk

import (
	"bufio"
	"os"
	"strconv"
)

const (
	blockSize   = 4096
	leafMaxSize = 30
)

type LeafBlock struct {
	// block id
	id          int64
	maxKey      int64
	nextBlockID int64
	prevBlockID int64
	kvsSize     int64
	kvs         []*KV
}

func NewLeafBlock(
	id int64,
	maxKey int64,
	prevBlockID int64,
	nextBlockID int64,
	kvsSize int64,
	kvs []*KV,
) *LeafBlock {
	return &LeafBlock{
		id:          id,
		maxKey:      maxKey,
		nextBlockID: nextBlockID,
		prevBlockID: prevBlockID,
		kvsSize:     kvsSize,
		kvs:         kvs,
	}
}

func (leaf *LeafBlock) ToBytes() []byte {
	if len(leaf.kvs) > leafMaxSize {
		panic("Too much Key Value Pair")
	}
	bytes := make([]byte, blockSize)
	bytes = bytes[0:0]
	bytes = append(bytes, []byte(strconv.FormatInt(leaf.id, 10)+byteSepString+
		strconv.FormatInt(leaf.maxKey, 10)+byteSepString+
		strconv.FormatInt(leaf.prevBlockID, 10)+byteSepString+
		strconv.FormatInt(leaf.nextBlockID, 10)+byteSepString+
		strconv.FormatInt(leaf.kvsSize, 10)+byteSepString,
	)...)
	for _, kv := range leaf.kvs {
		bytes = append(bytes, kv.ToBytes()...)
	}
	if len(bytes) > blockSize {
		panic("Node Size Too Large")
	}
	return bytes
}

func ReadLeafBlockFromDiskByBlockID(filename string, blockID int64) (*LeafBlock, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0777)
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
	prevBlockID, err := strconv.Atoi(readString[:len(readString)-1])
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
		int64(prevBlockID),
		int64(nextBlockID),
		byteSep, kvs), nil
}

func WriteLeafBlockFromDiskByBlockID(filename string, leaf *LeafBlock) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
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

func splitLeafNodeBlock(leaf *LeafBlock) {

}

func nextLeafNodeBlock() {}
