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
	return
}

func TestBPTree_Query(t *testing.T) {
	tree := disk.NewBPTree("test")

	//for i := 0; i < 1145; i++ {
	//	ok := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
	//	if !ok {
	//		t.Log(i)
	//	}
	//}

	// bug: 内存中的根节点和磁盘上的不一致
	// 显然 内存中的是正确的 但磁盘上并不是
	// 顺带一提 顺序写入会炸 但是非顺序居然不会
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

// 基本确定是index分块的时候炸了
// 另外
// 内存中的根节点和磁盘中的不同步
// 记得排查
// 现在同步了 因为更新父节点时候没有及时更新根节点
func TestBPTree_QueryAll(t *testing.T) {
	tree := disk.NewBPTree("test")
	for _, i := range rand.Perm(2000) {
		fmt.Println(tree.Query(int64(i)))
		//tree.Insert(int64(i), []byte(strconv.Itoa(i)))
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
