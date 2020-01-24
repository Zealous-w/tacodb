package store

import (
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDB struct {
	db   *leveldb.DB
	lock sync.Mutex
}

func NewLevelDB() IStore {
	return &LevelDB{}
}

func (c *LevelDB) Open(path string) error {
	option := &opt.Options{
		Filter:                 filter.NewBloomFilter(10),
		CompactionTableSize:    32 * 1024 * 1024,
		WriteL0SlowdownTrigger: 16,
		WriteL0PauseTrigger:    64,
		Compression:            opt.NoCompression,
		BlockSize:              32768,
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	db, err := leveldb.OpenFile(path, option)
	if err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *LevelDB) Close() {
	c.db.Close()
}

func (c *LevelDB) Put(tx interface{}, key, value []byte) error {
	t := tx.(*leveldb.Batch)
	t.Put(key, value)
	return nil
}

func (c *LevelDB) Get(tx interface{}, key []byte) []byte {
	ret, err := c.db.Get(key, &opt.ReadOptions{})
	if err != nil {
		return nil
	}
	return ret
}

func (c *LevelDB) Del(tx interface{}, key []byte) error {
	t := tx.(*leveldb.Batch)
	t.Delete(key)
	return nil
}

func (c *LevelDB) Transaction(f func(t interface{}) error) error {
	//c.lock.Lock()
	//defer c.lock.Unlock()
	batch := new(leveldb.Batch)
	err := f(batch)
	if err != nil {
		return err
	}
	return c.db.Write(batch, nil)
}

func (c *LevelDB) Scan(key []byte) []*Pair {
	ret := make([]*Pair, 0)
	it := c.db.NewIterator(util.BytesPrefix(key), nil)
	for it.Next() {
		ret = append(ret, &Pair{append([]byte{}, it.Key()...), append([]byte{}, it.Value()...)})
	}
	it.Release()
	err := it.Error()
	if err != nil {
		return nil
	}
	return ret
}

func (c *LevelDB) Range(start, end []byte) []*Pair {
	ret := make([]*Pair, 0)
	it := c.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	for it.Next() {
		ret = append(ret, &Pair{append([]byte{}, it.Key()...), append([]byte{}, it.Value()...)})
	}
	it.Release()
	err := it.Error()
	if err != nil {
		return nil
	}
	return ret
}
