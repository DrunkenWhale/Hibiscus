package main

import "strconv"

type KV struct {
	Key   int64
	Value Value
}

type Value interface {
	Len() int64
	ToBytes() []byte
	InjectFromBytes() // Value
}

func (kv *KV) ToBytes() []byte {
	return append([]byte(strconv.FormatInt(kv.Key, 10)+" "), kv.Value.ToBytes()...)
}
