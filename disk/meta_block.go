package disk

import (
	"bufio"
	"os"
	"strconv"
)

/* struct

tableName
nextLeafBlockID
nextIndexBlockID

??? unknown future


*/

const (
	tableMetaBlock = "data/table_meta_"
)

type TableMeta struct {
	tableName        string
	nextLeafBlockID  int64
	nextIndexBlockID int64
}

func (meta *TableMeta) ToBytes() []byte {
	return []byte((meta.tableName + byteSepString +
		strconv.FormatInt(meta.nextLeafBlockID, 10) + byteSepString +
		strconv.FormatInt(meta.nextIndexBlockID, 10) + byteSepString))
}

func (meta *TableMeta) WriteTableMeta() {
	file, err := os.OpenFile(tableMetaBlock+meta.tableName, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	_, err = file.Write(meta.ToBytes())
	if err != nil {
		panic(err)
	}
}

func ReadTableMeta(tableName string) *TableMeta {
	file, err := os.OpenFile(tableMetaBlock+tableName, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	meta := &TableMeta{}
	buf := bufio.NewReader(file)
	readString, err := buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil
	}
	meta.tableName = readString[:len(readString)-1]
	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil
	}
	nextLeafBlockID, err := strconv.ParseInt(readString[:len(readString)-1], 10, 64)
	if err != nil {
		panic(err)
		return nil
	}
	meta.nextLeafBlockID = nextLeafBlockID
	readString, err = buf.ReadString(byteSep)
	if err != nil {
		panic(err)
		return nil
	}
	nextIndexBlockID, err := strconv.ParseInt(readString[:len(readString)-1], 10, 64)
	if err != nil {
		panic(err)
		return nil
	}
	meta.nextIndexBlockID = nextIndexBlockID
	return meta
}

func NextLeafNodeBlockID(tableName string) int64 {
	meta := ReadTableMeta(tableName)
	res := meta.nextLeafBlockID
	meta.nextLeafBlockID++
	meta.WriteTableMeta()
	return res
}

func NextIndexNodeBlockID(tableName string) int64 {
	meta := ReadTableMeta(tableName)
	res := meta.nextIndexBlockID
	meta.nextIndexBlockID++
	meta.WriteTableMeta()
	return res
}
