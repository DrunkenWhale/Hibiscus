package disk

import "strconv"

type KI struct {
	Key   int64
	Index int64
}

func NewKI(key int64, index int64) *KI {
	return &KI{
		Key:   key,
		Index: index,
	}
}

func (ki *KI) ToBytes() []byte {
	return []byte(
		strconv.FormatInt(ki.Key, 10) + byteSepString +
			strconv.FormatInt(ki.Index, 10) + byteSepString,
	)
}
