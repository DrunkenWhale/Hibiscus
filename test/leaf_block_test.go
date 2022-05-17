package test

import (
	"Hibiscus/disk"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestLeafBlock_ToBytes(t *testing.T) {
	leaf := disk.NewLeafBlock(1, 77, 0, 3, []*disk.KV{
		disk.NewKV(114, []byte("514")),
		disk.NewKV(514, []byte("114")),
		disk.NewKV(114514, []byte("1919810")),
	})
	fmt.Println(string(leaf.ToBytes()))
}

func TestLeafBlock_Put(t *testing.T) {
	leaf := disk.NewLeafBlock(1, -1, 1, 0, []*disk.KV{})
	for _, num := range rand.Perm(31) {
		leaf.Put(int64(num), []byte(strconv.Itoa(num)))
	}
	return
}

func TestLeafBlock_Get(t *testing.T) {

	leaf := disk.NewLeafBlock(1, -1, 1, 0, []*disk.KV{})
	for _, num := range rand.Perm(31) {
		leaf.Put(int64(num), []byte(strconv.Itoa(num)))
	}
	for _, num := range rand.Perm(31) {
		ok, ans := leaf.Get(int64(num))
		fmt.Println(ok, num, "<==>", string(ans))
	}

}

func TestLeafBlock_Update(t *testing.T) {
	leaf := disk.NewLeafBlock(1, -1, 1, 0, []*disk.KV{})
	for _, num := range rand.Perm(31) {
		leaf.Put(int64(num), []byte(strconv.Itoa(num)))
	}
	fmt.Println(leaf.Update(11, []byte("917")))
	fmt.Println(leaf.Update(211, []byte("917")))
	for _, num := range rand.Perm(31) {
		ok, ans := leaf.Get(int64(num))
		fmt.Println(ok, num, "<==>", string(ans))
	}
}
