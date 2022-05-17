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

// Put
// return :
// false => Key exist
// true  => operation succeed
func (index *IndexBlock) Put(key int64, offset int64) bool {
	if len(index.kis) == 0 {
		index.kis = append(index.kis, NewKI(key, offset))
		index.childrenSize++
		return true
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.kis[mid].Key == key {
			return false // Key exist
		} else if index.kis[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	index.kis = append(index.kis, nil)
	copy(index.kis[left+1:], index.kis[left:])
	index.kis[left] = NewKI(key, offset)
	index.childrenSize++
	return true
}

// Get
// return :
// true  ,data => query data succeed, return data in second return data
// false ,nil  => query data failed, Key not existed
func (index *IndexBlock) Get(key int64) (bool, int64) {
	if len(index.kis) == 0 {
		return false, -1
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.kis[mid].Key == key {
			return true, index.kis[mid].Index
		} else if index.kis[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if index.kis[left].Key == key {
		return true, index.kis[left].Index
	}
	return false, -1
}

//Update
//return :
// false => Update failed, Key unexist
// ture  => Update succeed
func (index *IndexBlock) Update(key int64, offset int64) bool {
	if len(index.kis) == 0 {
		return false
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.kis[mid].Key == key {
			index.kis[mid].Index = offset
			return true
		} else if index.kis[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if index.kis[left].Key == key {
		index.kis[left].Index = offset
		return true
	}
	return false
}

func (index *IndexBlock) Delete(key int64) bool {
	if len(index.kis) == 0 {
		return false
	}
	if key < index.kis[0].Key {
		return false
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (right + left) >> 1
		if index.kis[mid].Key == key {
			index.childrenSize--
			index.kis = append(index.kis[:mid], index.kis[mid+1:]...)
			return true
		} else if index.kis[mid].Key > key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if index.kis[left].Key == key {
		index.childrenSize--
		index.kis = append(index.kis[:left], index.kis[left+1:]...)
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

func SplitIndexNodeBlock(index *IndexBlock, tableName string) (*IndexBlock, *IndexBlock) {
	stat, err := os.Stat(indexNodeDataStoragePrefix + tableName)
	if err != nil {
		panic(err)
	}
	nextBlockID := stat.Size() / blockSize
	newIndex := NewIndexBlock(nextBlockID, index.parent, 0, nil)
	bound := index.childrenSize / 2
	newIndexKIS := make([]*KI, indexChildrenMaxSize+1)
	for i := bound; i < index.childrenSize; i++ {
		newIndexKIS[i-bound] = index.kis[i]
		index.kis[i] = nil
	}
	newIndex.kis = newIndexKIS
	newIndex.childrenSize = index.childrenSize - bound
	index.childrenSize = bound
	return index, newIndex
}
