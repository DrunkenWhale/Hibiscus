package disk

import (
	"fmt"
	"testing"
)

func TestLeafBlock_ToBytes(t *testing.T) {
	leaf := NewLeafBlock(1, 77, 0, 3, []*KV{
		NewKV(114, []byte("514")),
		NewKV(514, []byte("114")),
		NewKV(114514, []byte("1919810")),
	})
	fmt.Println(string(leaf.ToBytes()))
}
