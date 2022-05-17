package disk

const (
	blockSize = 4096

	byteSep       = 3
	byteSepString = string(rune(byteSep))

	indexChildrenMaxSize       = 114
	indexNodeDataStoragePrefix = "index_"
)
