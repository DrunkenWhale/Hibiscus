package disk

import "strconv"

type KV struct {
	Key   int64
	Value []byte
}

func NewKV(key int64, value []byte) *KV {
	return &KV{
		Key:   key,
		Value: value,
	}
}

func (kv *KV) ToBytes() []byte {
	return append(
		append(
			[]byte(
				strconv.FormatInt(kv.Key, 10)+byteSepString),
			kv.Value...,
		),
		byteSep)
}
