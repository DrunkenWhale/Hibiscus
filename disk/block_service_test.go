package disk

import (
	"fmt"
	"testing"
)

func TestNextBlockID(t *testing.T) {
	t.Log(NextBlockID("test"))
}

func TestWriteLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf := NewLeafBlock(0, 77, 2, 3, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(114514, []byte("1919810")),
	})
	err := WriteLeafBlockFromDiskByBlockID("test", leaf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf, err := ReadLeafBlockFromDiskByBlockID("test", 1)
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
	fmt.Println(string(leaf1.ToBytes()))
	fmt.Println(string(leaf2.ToBytes()))
}
