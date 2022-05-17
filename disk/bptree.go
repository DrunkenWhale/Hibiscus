package disk

type BPTree struct {
	name string
	root *IndexBlock
}

func NewBPTree(name string) *BPTree {
	index, err := ReadIndexBlockFromDiskByBlockID(1, name)
	if err != nil {

		// block id equals zero
		// its parent will always pointer to root index node
		err := WriteIndexBlockToDiskByBlockID(
			NewIndexBlock(0, 1, 0, make([]*KI, 0)),
			name)
		if err != nil {
			panic(err)
		}
		index_ := NewIndexBlock(1, -1, 0, make([]*KI, 0))
		err = WriteIndexBlockToDiskByBlockID(
			index_,
			name)
		if err != nil {
			panic(err)
		}
		err = WriteLeafBlockToDiskByBlockID(
			NewLeafBlock(0, -114514, -1, 0, make([]*KV, 0)),
			name)
		if err != nil {
			panic(err)
		}
		return &BPTree{
			name: name,
			root: index_,
		}
	} else {
		return &BPTree{
			name: name,
			root: index,
		}
	}
}

func (tree *BPTree) Insert(key int64, value []byte) error {
	cursor := tree.root
	if cursor.childrenSize == 0 {
		// has no any index
		leaf, err := ReadLeafBlockFromDiskByBlockID(0, tree.name)
		if err != nil {
			return err
		}
		leaf.Put(key, value)
		err = WriteLeafBlockToDiskByBlockID(leaf, tree.name)
		if err != nil {
			return err
		}
	} else {

	}
	return nil
}
