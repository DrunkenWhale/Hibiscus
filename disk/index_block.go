package disk

type IndexBlock struct {
	id      uint64
	keys    []int64
	blockID []uint64
}
