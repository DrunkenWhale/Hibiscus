package disk

import (
	"fmt"
	"testing"
)

func TestReadIndexBlockFromDiskByBlockID(t *testing.T) {
	index, err := ReadIndexBlockFromDiskByBlockID(1, "test")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(index)
	for _, ki := range index.kis {
		t.Log(ki)
	}

}

func TestSplitIndexNodeBlock(t *testing.T) {
	index := NewIndexBlock(0, 2, 3, []*KI{
		NewKI(10, 2),
		NewKI(17, 3),
		NewKI(16, 4),
	})
	err := WriteIndexBlockToDiskByBlockID(index, "test")
	if err != nil {
		t.Log(err)
	}
	index1, index2 := SplitIndexNodeBlock(index, "test")
	err = WriteIndexBlockToDiskByBlockID(index1, "test")
	if err != nil {
		t.Log(err)
	}
	err = WriteIndexBlockToDiskByBlockID(index2, "test")
	if err != nil {
		t.Log(err)
	}
}

func TestIndexBlock_Put(t *testing.T) {
	index := NewIndexBlock(0, -1, 3, []*KI{
		NewKI(10, 2),
		NewKI(17, 3),
		NewKI(16, 4),
	})
	fmt.Println(index.Put(114, 514))
	fmt.Println(index.Put(0, 514))
	fmt.Println(index)
	index_ := NewIndexBlock(0, -1, 0, make([]*KI, 0))
	fmt.Println(index_.Put(114, 514))
	fmt.Println(index_)

}

func TestIndexBlock_Get(t *testing.T) {
	index, err := ReadIndexBlockFromDiskByBlockID(1, "test")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(index)
	for _, i := range index.kis {
		fmt.Println(i)
		index.Get(i.Key)
		fmt.Println(index.Get(i.Key))
	}
}
