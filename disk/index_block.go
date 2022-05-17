package disk

import (
	"bufio"
	"os"
	"strconv"
)

type IndexBlock struct {
	id           int64
	parent       int64
	childrenSize int64
	kis          []*KI
}

func NewIndexBlock(id int64, parent int64, childrenSize int64, kis []*KI) *IndexBlock {
	return &IndexBlock{
		id:           id,
		parent:       parent,
		childrenSize: childrenSize,
		kis:          kis,
	}
}

func (index *IndexBlock) ToBytes() []byte {
	if index.childrenSize > indexChildrenMaxSize {
		panic("Too much Key Index Pair")
	}
	bytes := make([]byte, blockSize)
	bytes = bytes[0:0]
	bytes = append(bytes, []byte(
		strconv.FormatInt(index.id, 10)+byteSepString+
			strconv.FormatInt(index.parent, 10)+byteSepString+
			strconv.FormatInt(index.childrenSize, 10)+byteSepString,
	)...)
	for _, ki := range index.kis {
		if ki == nil {
			break
		}
		bytes = append(bytes, ki.ToBytes()...)
	}
	if len(bytes) > blockSize {
		panic("Index Node Size Too Large")
	}
	return bytes[:4096]
}

func (index *IndexBlock) isRoot() bool {
	return index.id == -1
}

func (index *IndexBlock) Add(key int64, blockID int64) {
	//binary search
	for !index.isRoot() {

	}
}

func WriteIndexBlockToDiskByBlockID(index *IndexBlock, tableName string) error {
	file, err := os.OpenFile(indexNodeDataStoragePrefix+tableName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
		return err
	}
	defer file.Close()
	offset := index.id * blockSize
	_, err = file.WriteAt(index.ToBytes(), offset)
	if err != nil {
		panic(err)
		return err
	}
	return nil
}

func ReadIndexBlockFromDiskByBlockID(blockID int64, tableName string) (*IndexBlock, error) {
	file, err := os.OpenFile(indexNodeDataStoragePrefix+tableName, os.O_RDONLY, 0777)
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
	parent, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}
	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil, err
	}
	childrenSize, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
	}

	kis := make([]*KI, childrenSize)
	for i := 0; i < childrenSize; i++ {
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
		val, err := strconv.Atoi(readString[:len(readString)-1])
		if err != nil {
			panic(err)
			return nil, err
		}
		kis[i] = NewKI(int64(key), int64(val))
	}
	return NewIndexBlock(
		int64(id_),
		int64(parent),
		int64(childrenSize),
		kis), nil
}

func SplitIndexNodeBlock(index *IndexBlock) (*IndexBlock, *IndexBlock) {

}
