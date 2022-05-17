package disk

import (
	"strconv"
	"testing"
)

func TestNextBlockID(t *testing.T) {
	t.Log(NextLeafNodeBlockID("test"))
}

func TestWriteLeafBlockToDiskByBlockID(t *testing.T) {
	leaf := NewLeafBlock(0, 77, -1, 3, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
		NewKV(114514, []byte("1919810")),
	})
	err := WriteLeafBlockToDiskByBlockID(leaf, "test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf, err := ReadLeafBlockFromDiskByBlockID(1, "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range leaf.kvs {
		t.Log(kv)
	}
}

func TestSplitLeafNodeBlock(t *testing.T) {
	leaf := NewLeafBlock(0, 114514, 2, 7, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(1114, []byte("1919f810")),
		NewKV(11451, []byte("191981as0")),
		NewKV(11454, []byte("1919d810")),
		NewKV(11414, []byte("19198as10")),
		NewKV(114514, []byte("1919a810")),
	})
	leaf1, leaf2 := SplitLeafNodeBlock(leaf, "test")
	err := WriteLeafBlockToDiskByBlockID(leaf1, "test")
	if err != nil {
		t.Error(err)
	}
	err = WriteLeafBlockToDiskByBlockID(leaf2, "test")
	if err != nil {
		t.Error(err)
	}
}

func TestBlockReadAndWrite(t *testing.T) {
	kvs := make([]*KV, 0)
	for i := 1; i <= 30; i++ {
		kvs = append(kvs, NewKV(int64(i), []byte(strconv.Itoa(i))))
	}
	l := NewLeafBlock(0, 30, -1, 30, kvs)
	err := WriteLeafBlockToDiskByBlockID(l, "test")
	if err != nil {
		t.Log(err)
	}
	leaf, err := ReadLeafBlockFromDiskByBlockID(0, "test")
	if err != nil {
		t.Error(err)
	}
	leaf1, leaf2 := SplitLeafNodeBlock(leaf, "test")
	err = WriteLeafBlockToDiskByBlockID(leaf1, "test")
	if err != nil {
		t.Error(err)
	}
	err = WriteLeafBlockToDiskByBlockID(leaf2, "test")
	if err != nil {
		t.Error(err)
	}
}
