package test

import (
	"Hibiscus/disk"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	_ = disk.NewBPTree("test")
}

func TestBPTree_Insert(t *testing.T) {
	tree := disk.NewBPTree("test")
	tree.Insert(114, []byte("514"))
	for i := 0; i < 1145; i++ {
		ok := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
		if !ok {
			fmt.Println(i)
		}
	}
	//for _, i := range rand.Perm(500) {
	//	ok := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
	//	if !ok {
	//		fmt.Println(i)
	//	}
	//}
	return
}

func TestBPTree_Query(t *testing.T) {
	tree := disk.NewBPTree("test")
	for i := 0; i < 1145; i++ {
		ok := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
		if !ok {
			t.Log(i)
		}
	}
	for _, i := range rand.Perm(500) {
		ok, res := tree.Query(int64(i))
		if !ok {
			t.Error("can't find key " + strconv.Itoa(i))
		} else {
			t.Log(i, string(res))
		}
	}
	return
}

func TestBPTree_QueryAll(t *testing.T) {
	tree := disk.NewBPTree("test")
	for _, i := range rand.Perm(900) {
		tree.Insert(int64(i), []byte(strconv.Itoa(i)))
	}
	for _, kv := range tree.QueryAll() {
		t.Log(kv.Key, string(kv.Value))
	}
}

func TestBinarySearch(t *testing.T) {
	arr := []int{1, 5, 6, 7, 8, 9, 17, 27, 37}
	fmt.Println(arr[binarySearch(4, arr)])
	fmt.Println(arr[binarySearch(0, arr)])
	fmt.Println(arr[binarySearch(114514, arr)])
	fmt.Println(arr[binarySearch(20, arr)])
	fmt.Println(arr[binarySearch(12, arr)])
	fmt.Println(arr[binarySearch(7, arr)])
}

func binarySearch(key int, arr []int) int {
	if len(arr) == 0 {
		return -1
	}
	left := 0
	right := len(arr)
	for left < right {
		mid := (left + right) >> 1
		if arr[mid] < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == len(arr) {
		return left - 1
	}
	return left
}
