package test

import (
	"Hibiscus/disk"
	"strconv"
	"testing"
)

func TestNextBlockID(t *testing.T) {
	t.Log(disk.NextLeafNodeBlockID("test"))
}

func TestWriteLeafBlockToDiskByBlockID(t *testing.T) {
	leaf := disk.NewLeafBlock(0, 77, -1, 27, []*disk.KV{
		disk.NewKV(114, []byte("514")),
		disk.NewKV(514, []byte("114")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
		disk.NewKV(114514, []byte("1919810")),
	})
	err := disk.WriteLeafBlockToDiskByBlockID(leaf, "test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf, err := disk.ReadLeafBlockFromDiskByBlockID(0, "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range leaf.KVs {
		t.Log(kv)
	}
}

func TestSplitLeafNodeBlock(t *testing.T) {
	leaf := disk.NewLeafBlock(0, 114514, 2, 7, []*disk.KV{
		disk.NewKV(114, []byte("514")),
		disk.NewKV(514, []byte("114")),
		disk.NewKV(1114, []byte("1919f810")),
		disk.NewKV(11451, []byte("191981as0")),
		disk.NewKV(11454, []byte("1919d810")),
		disk.NewKV(11414, []byte("19198as10")),
		disk.NewKV(114514, []byte("1919a810")),
	})
	leaf1, leaf2 := disk.SplitLeafNodeBlock(leaf, "test")
	err := disk.WriteLeafBlockToDiskByBlockID(leaf1, "test")
	if err != nil {
		t.Error(err)
	}
	err = disk.WriteLeafBlockToDiskByBlockID(leaf2, "test")
	if err != nil {
		t.Error(err)
	}
}

func TestBlockReadAndWrite(t *testing.T) {
	kvs := make([]*disk.KV, 0)
	for i := 1; i <= 30; i++ {
		kvs = append(kvs, disk.NewKV(int64(i), []byte(strconv.Itoa(i))))
	}
	l := disk.NewLeafBlock(0, 30, -1, 30, kvs)
	err := disk.WriteLeafBlockToDiskByBlockID(l, "test")
	if err != nil {
		t.Log(err)
	}
	leaf, err := disk.ReadLeafBlockFromDiskByBlockID(0, "test")
	if err != nil {
		t.Error(err)
	}
	leaf1, leaf2 := disk.SplitLeafNodeBlock(leaf, "test")
	err = disk.WriteLeafBlockToDiskByBlockID(leaf1, "test")
	if err != nil {
		t.Error(err)
	}
	err = disk.WriteLeafBlockToDiskByBlockID(leaf2, "test")
	if err != nil {
		t.Error(err)
	}
}
