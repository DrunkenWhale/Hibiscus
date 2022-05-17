package disk

import "strconv"

type KI struct {
	key   int64
	index int64
}

func NewKI(key int64, index int64) *KI {
	return &KI{
		key:   key,
		index: index,
	}
}

func (ki *KI) ToBytes() []byte {
	return []byte(
		strconv.FormatInt(ki.key, 10) +
			byteSepString +
			strconv.FormatInt(ki.index, 10),
	)
}
