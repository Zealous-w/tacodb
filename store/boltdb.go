package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

const (
	BOLTDB_BUCKET_NAME = "data"
)

type BoltDB struct {
	db *bolt.DB
}

func NewBoltDB() IStore {
	return &BoltDB{}
}

func (d *BoltDB) Open(path string) error {
	var err error
	option := &bolt.Options{
		Timeout:    0,
		NoGrowSync: false,
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	d.db, err = bolt.Open(path+"/"+"data.db", 0600, option)
	if err != nil {
		return err
	}
	return nil
}

func (d *BoltDB) Close() {
	d.db.Close()
}

func (d *BoltDB) Put(tx interface{}, key, value []byte) error {
	b := tx.(*bolt.Bucket)
	return b.Put(key, value)
}

func (d *BoltDB) Get(tx interface{}, key []byte) []byte {
	b := tx.(*bolt.Bucket)
	return b.Get(key)
}

func (d *BoltDB) Del(tx interface{}, key []byte) error {
	b := tx.(*bolt.Bucket)
	return b.Delete(key)
}

func (d *BoltDB) Transaction(f func(t interface{}) error) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_NAME))
		if err != nil {
			return errors.New(fmt.Sprintf("not found bucket %+v", BOLTDB_BUCKET_NAME))
		}
		return f(b)
	})
}

func (d *BoltDB) Range(start, end []byte) (ret []*Pair) {
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET_NAME))
		if b == nil {
			return errors.New(fmt.Sprintf("not found bucket %+v", BOLTDB_BUCKET_NAME))
		}
		c := b.Cursor()
		for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
			ret = append(ret, &Pair{k, v})
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (d *BoltDB) Scan(key []byte) []*Pair {
	ret := make([]*Pair, 0)
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET_NAME))
		if b == nil {
			return errors.New(fmt.Sprintf("not found bucket %+v", BOLTDB_BUCKET_NAME))
		}
		c := b.Cursor()
		for k, v := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, v = c.Next() {
			ret = append(ret, &Pair{k, v})
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return ret
}
