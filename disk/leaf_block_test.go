package disk

import (
	"fmt"
	"testing"
)

func TestLeafBlock_ToBytes(t *testing.T) {
	leaf := NewLeafBlock(1, 77, 2, 0, 3, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(114514, []byte("1919810")),
	})
	fmt.Println(string(leaf.ToBytes()))
}

func TestWriteLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf := NewLeafBlock(1, 77, 2, 0, 3, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(114514, []byte("1919810")),
	})
	err := WriteLeafBlockFromDiskByBlockID("test", leaf, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadLeafBlockFromDiskByBlockID(t *testing.T) {
	leaf, err := ReadLeafBlockFromDiskByBlockID("test", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range leaf.kvs {
		t.Log(kv)
	}
}
