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
		index__ := NewLeafBlock(0, -114514, -1, 0, make([]*KV, 0))
		index__.parentIndex = 1
		err = WriteLeafBlockToDiskByBlockID(
			index__,
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

func (tree *BPTree) Insert(key int64, value []byte) bool {
	cursor := tree.root
	if cursor.childrenSize == 0 {
		// has no any index
		leaf, err := ReadLeafBlockFromDiskByBlockID(0, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		return tree.insertIntoLeafNodeAndWrite(key, value, leaf)
	} else {

	}
}

func (tree *BPTree) searchLeafNode(key int64) (bool, []byte) {

}

func (tree *BPTree) insertIntoLeafNodeAndWrite(key int64, value []byte, leaf *LeafBlock) bool {
	ok := leaf.Put(key, value)
	if !ok {
		return false
	}
	if leaf.kvsSize > leafNodeBlockMaxSize {
		leaf1, leaf2 := SplitLeafNodeBlock(leaf, tree.name)
		err := WriteLeafBlockToDiskByBlockID(leaf1, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		err = WriteLeafBlockToDiskByBlockID(leaf2, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		index, err := ReadIndexBlockFromDiskByBlockID(leaf.parentIndex, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		ok := tree.insertIntoIndexNodeAndWrite(leaf1.maxKey, leaf1.id, index)
		if !ok {
			return false
		}
		ok = tree.insertIntoIndexNodeAndWrite(leaf1.maxKey, leaf2.id, index)
		if !ok {
			return false
		}
		return true
	} else {
		err := WriteLeafBlockToDiskByBlockID(leaf, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		return true
	}
}

func (tree *BPTree) insertIntoIndexNodeAndWrite(key int64, blockID int64, index *IndexBlock) bool {
	ok := index.Put(key, blockID)
	if !ok {
		return false
	}
	for index.isFull() {
		index1, index2 := SplitIndexNodeBlock(index, tree.name)
		if index.isRoot() {
			newRoot := NewIndexBlock(NextIndexNodeBlockID(tree.name), -1, 0, make([]*KI, 0))
			index1.parent = newRoot.id
			index2.parent = newRoot.id
			newRoot.Put(index1.KIs[index1.childrenSize-1].Key, index1.id)
			newRoot.Put(index2.KIs[index2.childrenSize-1].Key, index2.id)
			tree.setRootNode(newRoot)
			err := WriteIndexBlockToDiskByBlockID(newRoot, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			err = WriteIndexBlockToDiskByBlockID(index1, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			err = WriteIndexBlockToDiskByBlockID(index2, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			index = newRoot
		} else {
			err := WriteIndexBlockToDiskByBlockID(
				index1,
				tree.name,
			)
			if err != nil {
				return false
			}
			err = WriteIndexBlockToDiskByBlockID(
				index2,
				tree.name,
			)
			if err != nil {
				return false
			}
			index_, err := ReadIndexBlockFromDiskByBlockID(index1.parent, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			index_.Put(index2.KIs[index2.childrenSize-1].Key, index2.id)
			err = WriteIndexBlockToDiskByBlockID(index_, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			index = index_
		}
	}
	return true
}

func (tree *BPTree) setRootNode(newRootIndex *IndexBlock) {
	root := NewIndexBlock(0, newRootIndex.id, 0, make([]*KI, 0))
	err := WriteIndexBlockToDiskByBlockID(root, tree.name)
	if err != nil {
		panic(err)
	}
	tree.root = root
}

func (tree *BPTree) getRootNode() int64 {
	index, err := ReadIndexBlockFromDiskByBlockID(0, tree.name)
	if err != nil {
		panic(err)
	}
	return index.parent
}
