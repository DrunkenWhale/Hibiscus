package disk

type BPTree struct {
	name string
	root *IndexBlock
}

func NewBPTree(name string) *BPTree {
	index, err := ReadIndexBlockFromDiskByBlockID(0, name)

	// root node unexist
	if err != nil {

		// block id equals zero
		// its parent will always pointer to root index node
		err := WriteIndexBlockToDiskByBlockID(
			NewIndexBlock(0, 0, 1, 0, make([]*KI, 0)),
			name)
		if err != nil {
			panic(err)
		}
		index_ := NewIndexBlock(1, 1, -1, 0, make([]*KI, 0))
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
		meta := &TableMeta{
			tableName:        name,
			nextLeafBlockID:  1,
			nextIndexBlockID: 2,
		}
		meta.WriteTableMeta()
		return &BPTree{
			name: name,
			root: index_,
		}
	} else {
		rootNode, err := ReadIndexBlockFromDiskByBlockID(index.parent, name)
		if err != nil {
			panic(err)
			return nil
		}
		return &BPTree{
			name: name,
			root: rootNode,
		}
	}
}

func (tree *BPTree) Query(key int64) (bool, []byte) {
	leaf := tree.searchLeafNode(key)
	ok, res := leaf.Get(key)
	if !ok {
		return false, nil
	} else {
		return true, res
	}
}

func (tree *BPTree) QueryAll() []*KV {
	res := make([]*KV, 0)
	nextBoundID := int64(0)
	for nextBoundID != -1 {
		leaf, err := ReadLeafBlockFromDiskByBlockID(nextBoundID, tree.name)
		if err != nil {
			panic(err)
		}
		res = append(res, leaf.KVs...)
		nextBoundID = leaf.nextBlockID
	}
	return res
}

func (tree *BPTree) Insert(key int64, value []byte) bool {
	// 从根节点开始向下查找
	cursor := tree.root
	// empty index
	// so data will be put in first leaf block
	// 当前是根节点且没有子节点的时候
	// 直接插入且不用建立索引
	if cursor.childrenSize == 0 {
		// has no any index
		// 这时候块0必定空闲
		// 所以从块0插入数据
		leaf, err := ReadLeafBlockFromDiskByBlockID(0, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		// 插入叶子结点的入口方法
		return tree.insertIntoLeafNodeAndWrite(key, value, leaf)

	} else {
		// 查找对应的叶子结点
		leaf := tree.searchLeafNode(key)

		return tree.insertIntoLeafNodeAndWrite(key, value, leaf)
	}
}

func (tree *BPTree) Delete(key int64) {
	leaf := tree.searchLeafNode(key)
	tree.deleteLeafNodeAndWrite(key, leaf)
}

func (tree *BPTree) searchLeafNode(key int64) *LeafBlock {
	cursor := tree.root
	// 游标不为叶子结点时
	for !cursor.isLeafIndex() {
		// 查找右边界
		rightBound := searchRightBoundFromIndexNode(key, cursor)
		if rightBound == -1 {
			panic("bug occurred")
			return nil
		}

		nextBlockID := cursor.KIs[rightBound].Index
		index, err := ReadIndexBlockFromDiskByBlockID(nextBlockID, tree.name)
		if err != nil {
			panic(err)
			return nil
		}
		cursor = index
	}
	rightBound := searchRightBoundFromIndexNode(key, cursor)
	if rightBound == -1 {
		panic("there must be a bug!")
		return nil
	}
	nextBoundID := cursor.KIs[rightBound].Index
	leaf, err := ReadLeafBlockFromDiskByBlockID(nextBoundID, tree.name)
	if err != nil {
		panic(err)
		return nil
	}
	return leaf
}

func (tree *BPTree) insertIntoLeafNodeAndWrite(key int64, value []byte, leaf *LeafBlock) bool {
	// 记录未更新的节点中的最大值
	oldMaxKey := leaf.maxKey
	// 放入添加的键值对
	ok := leaf.Put(key, value)
	// 添加失败即返回
	if !ok {
		return false
	}
	// 如果叶子节点的容量超过了上限
	// 进行切分
	if leaf.kvsSize > leafNodeBlockMaxSize {
		// 这里日后要改
		// 因为实质上
		// leaf1 和 leaf 指向同一个地址
		// 非常不函数式
		leaf1, leaf2 := SplitLeafNodeBlock(leaf, tree.name)
		// 分割后的节点分别写入磁盘
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
		// 从磁盘读取节点的索引节点的ID
		// 这样读取有问题
		index, err := ReadIndexBlockFromDiskByBlockID(leaf.parentIndex, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		// 通过索引值查找对应的键值
		// 这里也应该优化
		// 目前遍历的方式
		// 是O(n)的
		oldKey := tree.getIndexKeyByOffsetID(leaf1.id, index)
		// 删除指向当前叶子结点的旧索引
		index.Delete(oldKey)
		// 将索引刷写入磁盘中
		err = WriteIndexBlockToDiskByBlockID(index, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		// 用于插入索引
		tree.insertIntoIndexNodeAndWrite(leaf1.maxKey, leaf1.id, index)
		tree.insertIntoIndexNodeAndWrite(leaf2.maxKey, leaf2.id, index)
		// 如果索引是索引树的根节点
		// 需要实时更新
		if index.isRoot() {
			// root node always stay in memory
			// must update it at once if its block message change
			tree.root = index

		}
		return true
	} else {
		// 刷写进磁盘
		err := WriteLeafBlockToDiskByBlockID(leaf, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		if key > oldMaxKey {
			index, err := ReadIndexBlockFromDiskByBlockID(leaf.parentIndex, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			oldKey := tree.getIndexKeyByOffsetID(leaf.id, index)
			index.Delete(oldKey)
			err = WriteIndexBlockToDiskByBlockID(index, tree.name)
			if err != nil {
				panic(err)
				return false
			}
			tree.insertIntoIndexNodeAndWrite(key, leaf.id, index)
		}
		return true
	}
}

func (tree *BPTree) insertIntoIndexNodeAndWrite(key int64, blockID int64, index *IndexBlock) bool {
	// 向索引中添加数据
	ok := index.Put(key, blockID)
	if !ok {
		return false
	}
	// 刷写到磁盘
	err := WriteIndexBlockToDiskByBlockID(index, tree.name)
	if err != nil {
		panic(err)
		return false
	}
	// 向上回溯直到根节点
	for !index.isRoot() {
		// 读取父亲索引
		index_, err := ReadIndexBlockFromDiskByBlockID(index.parent, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		// 当前索引满了
		// 进行分裂
		if index.isFull() {
			index1, index2 := SplitIndexNodeBlock(index, tree.name)
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
			// remove old index which point to node before spilt
			// 更新父亲索引
			// 必须先删除旧的索引
			oldKey := tree.getIndexKeyByOffsetID(index1.id, index_)
			index_.Delete(oldKey)
			index_.Put(index1.KIs[index1.childrenSize-1].Key, index1.id)
			index_.Put(index2.KIs[index2.childrenSize-1].Key, index2.id)

			//更新子节点
			if index.isLeafIndex() {
				for _, ki := range index2.KIs {
					needUpdateLeaf, err := ReadLeafBlockFromDiskByBlockID(ki.Index, tree.name)
					if err != nil {
						panic(err)
						return false
					}
					needUpdateLeaf.parentIndex = index2.id
					err = WriteLeafBlockToDiskByBlockID(needUpdateLeaf, tree.name)
					if err != nil {
						panic(err)
						return false
					}
				}
			} else {
				for _, ki := range index2.KIs {
					needUpdateIndex, err := ReadIndexBlockFromDiskByBlockID(ki.Index, tree.name)
					if err != nil {
						panic(err)
						return false
					}
					needUpdateIndex.parent = index2.id
					err = WriteIndexBlockToDiskByBlockID(needUpdateIndex, tree.name)
					if err != nil {
						panic(err)
						return false
					}
				}
			}

		} else {
			// 父亲索引节点没满
			// 获取旧索引值
			// 如果旧索引的key值更小
			// 就要更新
			oldKey := tree.getIndexKeyByOffsetID(index.id, index_)
			if oldKey < key {
				if oldKey == -1 {
					panic("Illegal Block")
				} else {
					index_.Delete(oldKey)
					index_.Put(key, index.id)
					if index_.isRoot() {
						tree.root = index_
					}
				}
			}
		}
		// 刷写进入磁盘
		err = WriteIndexBlockToDiskByBlockID(index_, tree.name)
		if err != nil {
			panic(err)
			return false
		}
		index = index_
		if index_.isRoot() {
			tree.root = index_
		}
	}
	if index.isFull() {
		index1, index2 := SplitIndexNodeBlock(index, tree.name)
		newRoot := NewIndexBlock(NextIndexNodeBlockID(tree.name), 0, -1, 0, make([]*KI, 0))
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
		//更新子节点
		if index.isLeafIndex() {
			for _, ki := range index2.KIs {
				needUpdateLeaf, err := ReadLeafBlockFromDiskByBlockID(ki.Index, tree.name)
				if err != nil {
					panic(err)
					return false
				}
				needUpdateLeaf.parentIndex = index2.id
				err = WriteLeafBlockToDiskByBlockID(needUpdateLeaf, tree.name)
				if err != nil {
					panic(err)
					return false
				}
			}
		} else {
			for _, ki := range index2.KIs {
				needUpdateIndex, err := ReadIndexBlockFromDiskByBlockID(ki.Index, tree.name)
				if err != nil {
					panic(err)
					return false
				}
				needUpdateIndex.parent = index2.id
				err = WriteIndexBlockToDiskByBlockID(needUpdateIndex, tree.name)
				if err != nil {
					panic(err)
					return false
				}
			}
		}
		index = newRoot
	}
	return true
}

func (tree *BPTree) deleteLeafNodeAndWrite(key int64, leaf *LeafBlock) {
	index, err := ReadIndexBlockFromDiskByBlockID(leaf.parentIndex, tree.name)
	if err != nil {
		panic(err)
	}
	leaf.Delete(key)
	err = WriteLeafBlockToDiskByBlockID(leaf, tree.name)
	if err != nil {
		panic(err)
	}
	if leaf.kvsSize == 0 {
		tree.deleteIndexNodeAndWrite(key, index)
	}
}

func (tree *BPTree) deleteIndexNodeAndWrite(key int64, index *IndexBlock) {
	if key == index.KIs[index.childrenSize-1].Key {
		cursor := index
		for !cursor.isRoot() {
			ok := cursor.Delete(key)
			if !ok {
				return
			}
			cursor.Delete(key)
			err := WriteIndexBlockToDiskByBlockID(index, tree.name)
			if err != nil {
				panic(err)
			}
			_index, err := ReadIndexBlockFromDiskByBlockID(cursor.parent, tree.name)
			if err != nil {
				panic(err)
			}
			cursor = _index
		}
		ok := cursor.Delete(key)
		if !ok {
			return
		}
		cursor.Delete(key)
		err := WriteIndexBlockToDiskByBlockID(index, tree.name)
		if err != nil {
			panic(err)
		}
		tree.root = cursor
	} else {
		return
	}
}

func (tree *BPTree) setRootNode(newRootIndex *IndexBlock) {
	root := NewIndexBlock(0, 0, newRootIndex.id, 0, make([]*KI, 0))
	err := WriteIndexBlockToDiskByBlockID(root, tree.name)
	if err != nil {
		panic(err)
	}
	tree.root = newRootIndex
}

func (tree *BPTree) getIndexKeyByOffsetID(offset int64, index *IndexBlock) int64 {
	for _, ki := range index.KIs {
		if ki.Index == offset {
			return ki.Key
		}
	}
	return -1
}

func getRootNode(tableName string) int64 {
	index, err := ReadIndexBlockFromDiskByBlockID(0, tableName)
	if err != nil {
		panic(err)
	}
	return index.parent
}

func searchRightBoundFromIndexNode(key int64, index *IndexBlock) int64 {
	if len(index.KIs) == 0 {
		return -1
	}
	left := int64(0)
	right := index.childrenSize
	for left < right {
		mid := (left + right) >> 1
		if index.KIs[mid].Key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == int64(len(index.KIs)) {
		return left - 1
	}
	return left
}
