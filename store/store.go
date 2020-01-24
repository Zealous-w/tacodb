package store

import (
	"fmt"
)

const (
	CONST_STORE_NUM = 4 //2^CONST_STORE_NUM
)

type Pair struct {
	V0 []byte
	V1 []byte
}

type IStore interface {
	Open(path string) error
	Close()
	Put(tx interface{}, key, value []byte) error
	Get(tx interface{}, key []byte) []byte
	Del(tx interface{}, key []byte) error
	Transaction(func(t interface{}) error) error
	Scan(key []byte) []*Pair
	Range(start, end []byte) []*Pair //[start, end)
}

func NewDBStore(engine, path string) ([]IStore, func()) {
	ret := make([]IStore, 0, 1<<CONST_STORE_NUM)
	for i:=0; i < 1<<CONST_STORE_NUM; i++ {
		switch engine {
		case "boltdb":
			ret = append(ret, NewBoltDB())
		case "leveldb":
			ret = append(ret, NewLevelDB())
		}
		err := ret[i].Open(path + "/" + engine + "/" + fmt.Sprintf("/%d", i))
		if err != nil {
			panic(fmt.Sprintf("open db failed, index=%d, err=%+v", i, err))
		}
	}
	close := func() {
		for _, v := range ret {
			v.Close()
		}
	}
 	return ret, close
}
