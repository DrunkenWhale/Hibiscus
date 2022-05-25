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
	for i := 0; i < 114514; i++ {
		ok := tree.Insert(int64(i), []byte(strconv.Itoa(i)))
		if !ok {
			fmt.Println(i)
		}
	}
	return
}

func TestBPTree_Query(t *testing.T) {
	tree := disk.NewBPTree("test")
	for _, i := range rand.Perm(2000) {
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
	for _, kv := range tree.QueryAll() {
		t.Log(kv.Key, string(kv.Value))
	}
}

func TestBPTree_Delete(t *testing.T) {
	tree := disk.NewBPTree("test")
	for _, i := range rand.Perm(2000) {
		tree.Delete(int64(i))
	}
	tree.Insert(1134, []byte("may be you can find this"))
	for _, i := range rand.Perm(2000) {
		ok, res := tree.Query(int64(i))
		if !ok {
			t.Log("can't find key " + strconv.Itoa(i))
		} else {
			t.Log(i, string(res))
		}
	}
}

func TestBPTree_CRUD1(t *testing.T) {
	tree := disk.NewBPTree("test")
	//tree.Insert(114514, []byte("1919810"))
	tree.Insert(114514, []byte("1919810"))
	t.Log(tree.QueryAll())
	t.Log(tree.Query(114514))
	t.Log(tree.Query(1145))
	t.Log(tree.Delete(114514))
	t.Log(tree.Query(114514))
}
func TestBPTree_CRUD2(t *testing.T) {
	tree := disk.NewBPTree("test")
	randomArray := rand.Perm(114514)
	for i := 0; i < len(randomArray); i++ {
		if randomArray[i]%2 == 0 {
			//res := tree.Delete(rand.Int63())
			//t.Logf("Delete :%v", res)
		} else if randomArray[i]%3 == 0 {
			res, err := tree.Query(rand.Int63())
			t.Logf("Query :%v %v", res, err)
		} else {
			res := tree.Insert(rand.Int63(), []byte(strconv.Itoa(randomArray[i])))
			t.Logf("Insert :%v", res)
		}
	}
	//tree.Insert(114514, []byte("1919810"))
	tree.Insert(114514, []byte("1919810"))
	t.Log(tree.QueryAll())
	t.Log(tree.Query(114514))
	t.Log(tree.Query(1145))
	t.Log(tree.Delete(114514))
	t.Log(tree.Query(114514))
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
