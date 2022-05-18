package disk

import (
	"bufio"
	"os"
	"strconv"
)

const (
	indexChildrenMaxSize       = 50
	indexNodeDataStoragePrefix = "data/index_"
)

type IndexBlock struct {
	id           int64
	isLeaf       int64
	parent       int64
	childrenSize int64
	KIs          []*KI
}

func NewIndexBlock(id int64, isLeaf int64, parent int64, childrenSize int64, kis []*KI) *IndexBlock {
	return &IndexBlock{
		id:           id,
		isLeaf:       isLeaf,
		parent:       parent,
		childrenSize: childrenSize,
		KIs:          kis,
	}
}

func (index *IndexBlock) ToBytes() []byte {
	if index.childrenSize > indexChildrenMaxSize+1 {
		panic("Too much Key Index Pair")
	}
	bytes := make([]byte, blockSize)
	bytes = bytes[0:0]
	bytes = append(bytes, []byte(
		strconv.FormatInt(index.id, 10)+byteSepString+
			strconv.FormatInt(index.isLeaf, 10)+byteSepString+
			strconv.FormatInt(index.parent, 10)+byteSepString+
			strconv.FormatInt(index.childrenSize, 10)+byteSepString,
	)...)
	for _, ki := range index.KIs {
		if ki == nil {
			break
		}
		bytes = append(bytes, ki.ToBytes()...)
	}
	if len(bytes) > blockSize {
		panic("Index Node Size Too Large")
	}
	return bytes[:blockSize]
}

func (index *IndexBlock) isRoot() bool {
	return index.parent == -1
}

func (index *IndexBlock) isFull() bool {
	return index.childrenSize > indexChildrenMaxSize
}

func (index *IndexBlock) isLeafIndex() bool {
	return index.isLeaf == 1
}

// Put
// return :
// false => Key exist
// true  => operation succeed
func (index *IndexBlock) Put(key int64, blockID int64) bool {
	if len(index.KIs) == 0 {
		index.KIs = append(index.KIs, NewKI(key, blockID))
		index.childrenSize++
		return true
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.KIs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == index.childrenSize {
		index.KIs = append(index.KIs, NewKI(key, blockID))
		index.childrenSize++
		return true
	}

	if index.KIs[left].Key == key {
		// key index exist
		return false
	}
	index.KIs = append(index.KIs, nil)
	copy(index.KIs[left+1:], index.KIs[left:])
	index.KIs[left] = NewKI(key, blockID)
	index.childrenSize++
	return true
}

// Get
// return :
// true  ,data => query data succeed, return data in second return data
// false ,nil  => query data failed, Key not existed
func (index *IndexBlock) Get(key int64) (bool, int64) {
	if len(index.KIs) == 0 {
		return false, -1
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.KIs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == index.childrenSize {
		return false, -1
	}
	if index.KIs[left].Key == key {
		return true, index.KIs[left].Index
	}
	return false, -1
}

//Update
//return :
// false => Update failed, Key unexist
// ture  => Update succeed
func (index *IndexBlock) Update(key int64, offset int64) bool {
	if len(index.KIs) == 0 {
		return false
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.KIs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == index.childrenSize {
		return false
	}
	if index.KIs[left].Key == key {
		index.KIs[left].Index = offset
		return true
	}
	return false
}

func (index *IndexBlock) Delete(key int64) bool {
	if len(index.KIs) == 0 {
		return false
	}
	if key < index.KIs[0].Key {
		return false
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.KIs[mid].Key >= key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if left == index.childrenSize {
		return false
	}
	if index.KIs[left].Key == key {
		index.childrenSize--
		index.KIs = append(index.KIs[:left], index.KIs[left+1:]...)
		return true
	}
	return false
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
	isLeaf, err := strconv.Atoi(readString[:len(readString)-1])
	if err != nil {
		panic(err)
		return nil, err
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
		int64(isLeaf),
		int64(parent),
		int64(childrenSize),
		kis), nil
}

func SplitIndexNodeBlock(index *IndexBlock, tableName string) (*IndexBlock, *IndexBlock) {
	nextBlockID := NextIndexNodeBlockID(tableName)
	newIndex := NewIndexBlock(nextBlockID, index.isLeaf, index.parent, 0, nil)
	bound := index.childrenSize / 2
	newIndexKIS := make([]*KI, indexChildrenMaxSize+1)
	for i := bound; i < index.childrenSize; i++ {
		newIndexKIS[i-bound] = index.KIs[i]
		index.KIs[i] = nil
	}
	newIndex.KIs = newIndexKIS
	newIndex.childrenSize = index.childrenSize - bound
	index.childrenSize = bound
	return index, newIndex
}
