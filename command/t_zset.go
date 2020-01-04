package command

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type ZSetMeta struct {
	len uint32
}

func (m *ZSetMeta) Encode(data []byte) {
	if len(data) < 4 {
		return
	}
	m.len = binary.LittleEndian.Uint32(data)
}

func (m *ZSetMeta) Decode() []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, m.len)
	return ret
}

//type-key_size-key-score
func (*RedisCommand) ZSetEncodeKey(key []byte, score uint64, value []byte) []byte {
	ret := make([]byte, 1+4+len(key)+8+len(value))
	ret[0] = KEY_TYPE_ZSET_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	binary.BigEndian.PutUint64(ret[1+4+len(key):], score)
	copy(ret[1+4+len(key)+8:], value)
	return ret
}

func (*RedisCommand) ZSetDecodeKey(data []byte) (uint64, []byte) {
	if len(data) > 1 && data[0] != KEY_TYPE_ZSET_FIELD {
		return 0, nil
	}
	if len(data[1:]) < 4 {
		return 0, nil
	}
	keyLen := binary.LittleEndian.Uint32(data[1:])
	if len(data) < int(1+4+keyLen+8) {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data[1+4+keyLen:]), data[1+4+keyLen+8:]
}

func (*RedisCommand) ZSetEncodeKeyPrefix(key []byte, score uint64) []byte {
	ret := make([]byte, 1+4+len(key)+8)
	ret[0] = KEY_TYPE_ZSET_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	binary.BigEndian.PutUint64(ret[1+4+len(key):], score)
	return ret
}

func (*RedisCommand) ZSetEncodeScoreKey(key, value []byte) []byte {
	ret := make([]byte, 1+4+len(key)+4+len(value))
	ret[0] = KEY_TYPE_ZSET_SCORE
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	binary.LittleEndian.PutUint32(ret[1+4+len(key):], uint32(len(value)))
	copy(ret[1+4+len(key)+4:], value)
	return ret
}

func (*RedisCommand) ZSetEncodeScoreKeyPrefix(key []byte) []byte {
	ret := make([]byte, 1+4+len(key))
	ret[0] = KEY_TYPE_ZSET_SCORE
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	return ret
}

func (c *RedisCommand) ZSetDel(key []byte) error {
	return c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		ret := c.db.Get(t, metaKey)
		if ret == nil {
			return ErrKeyNotFound
		}
		_ = c.db.Del(t, metaKey)
		fields := c.db.Range(c.ZSetEncodeKeyPrefix(key, 0), c.ZSetEncodeKeyPrefix(key, 2<<63-1))
		for _, v := range fields {
			_ = c.db.Del(t, v.V0)
		}
		scores := c.db.Scan(c.ZSetEncodeScoreKeyPrefix(key))
		for _, v := range scores {
			_ = c.db.Del(t, v.V0)
		}
		return nil
	})
}

func (c *RedisCommand) ZAdd(key []byte, score uint64, value []byte) error {
	return c.db.Transaction(func(t interface{}) error {
		meta := &ZSetMeta{}
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if expire {
			_ = c.ZSetDel(key)
		}

		oldScore := c.db.Get(t, c.ZSetEncodeScoreKey(key, value))
		if oldScore != nil { //delete old node
			err := c.db.Del(t, c.ZSetEncodeScoreKey(key, value))
			if err != nil {
				return err
			}
			err = c.db.Del(t, c.ZSetEncodeKey(key, binary.LittleEndian.Uint64(oldScore), value))
			if err != nil {
				return err
			}
			meta.len--
		}

		meta.Encode(data)
		meta.len++
		err := c.db.Put(t, metaKey, c.EncodeValue(meta.Decode(), 0))
		if err != nil {
			return err
		}
		scoreByte := make([]byte, 8)
		binary.LittleEndian.PutUint64(scoreByte, score)
		err = c.db.Put(t, c.ZSetEncodeScoreKey(key, value), scoreByte)
		if err != nil {
			return err
		}
		return c.db.Put(t, c.ZSetEncodeKey(key, score, value), value)
	})
}

func (c *RedisCommand) ZRem(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		meta := &ZSetMeta{}
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		meta.Encode(data)

		var err error
		for _, field := range args {
			meta.len--
			scoreKey := c.ZSetEncodeScoreKey(key, field)
			data := c.db.Get(t, scoreKey)
			if len(data) <= 0 {
				return ErrKeyTypeError
			}
			err = c.db.Del(t, scoreKey)
			if err != nil {
				return err
			}
			err = c.db.Del(t, c.ZSetEncodeKey(key, binary.LittleEndian.Uint64(data), field))
			if err != nil {
				return err
			}
		}

		return c.db.Put(t, metaKey, meta.Decode())
	})
}

func (c *RedisCommand) ZScore(key, value []byte) (ret []byte) {
	err := c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		ret = c.db.Get(t, c.ZSetEncodeScoreKey(key, value))
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) ZIncrby(key []byte, args ...[]byte) (ret []byte, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		addScore, err := strconv.ParseUint(string(args[0]), 10, 64)
		if err != nil {
			return err
		}

		oldScore := c.db.Get(t, c.ZSetEncodeScoreKey(key, args[1]))
		if oldScore == nil {
			return ErrKeyNotFound
		}
		score := binary.LittleEndian.Uint64(oldScore)
		_ = c.db.Del(t, c.ZSetEncodeKey(key, score, args[1]))
		err = c.db.Put(t, c.ZSetEncodeKey(key, score+addScore, args[1]), args[1])
		if err != nil {
			return err
		}
		byteScore := make([]byte, 8)
		binary.LittleEndian.PutUint64(byteScore, score+addScore)
		ret = []byte(fmt.Sprintf("%d", score+addScore))
		return c.db.Put(t, c.ZSetEncodeScoreKey(key, args[1]), byteScore)
	})
	return
}

func (c *RedisCommand) ZRange(key []byte, args ...[]byte) (ret [][]byte) {
	err := c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		showScore := false
		if len(args) > 2 && strings.ToUpper(string(args[2])) == "WITHSCORES" {
			showScore = true
		}
		slc := c.db.Range(c.ZSetEncodeKeyPrefix(key, 0), c.ZSetEncodeKeyPrefix(key, 2<<63-1))
		for _, v := range slc {
			ret = append(ret, v.V1)
			if showScore {
				score, _ := c.ZSetDecodeKey(v.V0)
				ret = append(ret, []byte(fmt.Sprintf("%d", score)))
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) ZRank(key, value []byte) (ret int, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}

		v := c.db.Get(t, c.ZSetEncodeScoreKey(key, value))
		if v == nil {
			return ErrKeyNotFound
		}
		slc := c.db.Range(c.ZSetEncodeKeyPrefix(key, 0), c.ZSetEncodeKeyPrefix(key, 2<<63-1))
		for k, v := range slc {
			if bytes.Compare(v.V1, value) == 0 {
				ret = k
				break
			}
		}
		return nil
	})
	return
}

func (c *RedisCommand) ZCount(key []byte, args ...[]byte) (ret int, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}

		start, err := strconv.ParseUint(string(args[0]), 10, 64)
		if err != nil {
			return err
		}

		end, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return err
		}

		slc := c.db.Range(c.ZSetEncodeKeyPrefix(key, start), c.ZSetEncodeKeyPrefix(key, end))
		ret = len(slc)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return
}

func (c *RedisCommand) ZRevRange(key []byte, args ...[]byte) (ret [][]byte) {
	err := c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		showScore := false
		if len(args) > 2 && strings.ToUpper(string(args[2])) == "WITHSCORES" {
			showScore = true
		}
		slc := c.db.Range(c.ZSetEncodeKeyPrefix(key, 0), c.ZSetEncodeKeyPrefix(key, 2<<63-1))
		for k := range slc {
			v := slc[len(slc)-k-1]
			ret = append(ret, v.V1)
			if showScore {
				score, _ := c.ZSetDecodeKey(v.V0)
				ret = append(ret, []byte(fmt.Sprintf("%d", score)))
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) ZCard(key []byte) (ret int, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_ZSET, key)
		expire, data := c.DecodeValue(c.db.Get(t, metaKey))
		if data == nil {
			return ErrKeyNotFound
		}
		if expire {
			_ = c.ZSetDel(key)
			return ErrKeyNotFound
		}
		meta := &ZSetMeta{}
		meta.Encode(data)
		ret = int(meta.len)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return
}
