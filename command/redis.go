package command

import (
	"encoding/binary"
	"errors"
	"github.com/Zealous-w/tacodb/store"
	"github.com/Zealous-w/tacodb/util"
	"time"
)

const (
	KEY_TYPE_STRING     = 'C' //string
	KEY_TYPE_HASH       = 'H' //hash
	KEY_TYPE_HASH_FIELD = 'I' //hash field
	KEY_TYPE_LIST       = 'L' //list
	KEY_TYPE_LIST_FIELD = 'M' //list field
	KEY_TYPE_SET        = 'S' //set
	KEY_TYPE_SET_FIELD  = 'T' //set field
	KEY_TYPE_ZSET       = 'Z' //zset
	KEY_TYPE_ZSET_FIELD = 'A' //zset field
	KEY_TYPE_ZSET_SCORE = 'B' //zset score field
)

const (
	VALUE_META_LEN = 4
)

var (
	ErrKeyTypeError = errors.New("key type is invalid")
	ErrKeyNotFound  = errors.New("key not found")
)

type RedisCommand struct {
	db     []store.IStore
}

func NewRedisCommand(db []store.IStore) *RedisCommand {
	return &RedisCommand{
		db: db,
	}
}

func (c *RedisCommand) DB(key []byte) store.IStore {
	if len(c.db) == 0 {
		return nil
	}
	//index := util.BKDRHash(key) & uint32(len(c.db) - 1)
	index := util.BKDRHash(key) % uint32(len(c.db))
	return c.db[index]
}

func (*RedisCommand) EncodeKey(tp byte, key []byte) []byte {
	ret := make([]byte, len(key)+1)
	ret[0] = tp
	copy(ret[1:], key)
	return ret
}

func (*RedisCommand) EncodeValue(value []byte, ttl uint32) []byte {
	ret := make([]byte, len(value)+VALUE_META_LEN)
	timestamp := uint32(0)
	if ttl > 0 {
		timestamp = uint32(time.Now().Unix()) + ttl
	}
	binary.LittleEndian.PutUint32(ret, timestamp)
	copy(ret[4:], value)
	return ret
}

func (*RedisCommand) DecodeValue(value []byte) (bool, []byte) {
	if len(value) == 0 {
		return false, nil
	}
	timestamp := binary.LittleEndian.Uint32(value)
	if timestamp == 0 {
		return false, value[4:]
	}

	if timestamp < uint32(time.Now().Unix()) {
		return true, nil
	}
	return false, value[4:]
}
