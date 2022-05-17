package test

import (
	"Hibiscus/disk"
	"fmt"
	"testing"
)

func TestReadIndexBlockFromDiskByBlockID(t *testing.T) {
	index, err := disk.ReadIndexBlockFromDiskByBlockID(1, "test")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(index)
	for _, ki := range index.KIs {
		t.Log(ki)
	}

}

func TestSplitIndexNodeBlock(t *testing.T) {
	index := disk.NewIndexBlock(0, 2, 3, []*disk.KI{
		disk.NewKI(10, 2),
		disk.NewKI(17, 3),
		disk.NewKI(16, 4),
	})
	err := disk.WriteIndexBlockToDiskByBlockID(index, "test")
	if err != nil {
		t.Log(err)
	}
	index1, index2 := disk.SplitIndexNodeBlock(index, "test")
	err = disk.WriteIndexBlockToDiskByBlockID(index1, "test")
	if err != nil {
		t.Log(err)
	}
	err = disk.WriteIndexBlockToDiskByBlockID(index2, "test")
	if err != nil {
		t.Log(err)
	}
}

func TestIndexBlock_Put(t *testing.T) {
	index := disk.NewIndexBlock(0, -1, 3, []*disk.KI{
		disk.NewKI(10, 2),
		disk.NewKI(17, 3),
		disk.NewKI(16, 4),
	})
	fmt.Println(index.Put(114, 514))
	fmt.Println(index.Put(0, 514))
	fmt.Println(index)
	index_ := disk.NewIndexBlock(0, -1, 0, make([]*disk.KI, 0))
	fmt.Println(index_.Put(114, 514))
	fmt.Println(index_)

}

func TestIndexBlock_Get(t *testing.T) {
	index, err := disk.ReadIndexBlockFromDiskByBlockID(1, "test")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(index)
	for _, i := range index.KIs {
		fmt.Println(i)
		index.Get(i.Key)
		fmt.Println(index.Get(i.Key))
	}
}
