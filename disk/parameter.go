package disk

import "os"

const (
	blockSize = 4096 * 4

	fillByte             = 2
	byteSep              = 3
	byteSepString        = string(rune(byteSep))
	storageDirectoryName = "data"
)

var (
	deleteOperationMarkBytes = []byte{1, 1, 4, 5, 1, 4}
)

func init() {
	_ = os.Mkdir(storageDirectoryName, 666)
}
