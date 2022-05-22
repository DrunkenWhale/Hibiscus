# Hibiscus

key-value storage base on disk

it uses `B+ Tree` storage data

Slow write and efficient query

~~terrible code makes I/O too much~~

```go

package test

import (
	"Hibiscus/disk"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

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
```