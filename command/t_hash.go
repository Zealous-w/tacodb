package command

import (
	"encoding/binary"
	"tacodb/store"
)

//hash
func (*RedisCommand) HashEncodeKey(key, field []byte) []byte {
	ret := make([]byte, 1+4+len(key)+4+len(field)) //type-key_size-key-field
	ret[0] = KEY_TYPE_HASH_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[5:], key)
	binary.LittleEndian.PutUint32(ret[5+len(key):], uint32(len(field)))
	copy(ret[5+len(key)+4:], field)
	return ret
}

func (*RedisCommand) HashEncodePrefix(key []byte) []byte {
	ret := make([]byte, 1+4+len(key)) //type-key_size-key-field
	ret[0] = KEY_TYPE_HASH_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[5:], key)
	return ret
}

func (*RedisCommand) HashDecodeKey(key []byte) []byte {
	if len(key) < 1 {
		return nil
	}
	tp := key[0]
	if tp != KEY_TYPE_HASH_FIELD {
		return nil
	}
	if len(key[1:]) < 4 {
		return nil
	}
	kLen := binary.LittleEndian.Uint32(key[1:])
	if len(key[5:]) < int(kLen) {
		return nil
	}
	//fLen := binary.LittleEndian.Uint32(key[5+kLen:])
	return key[5+kLen+4:]
}

func (c *RedisCommand) HashDel(key []byte) error {
	return c.db.Transaction(func(t interface{}) error {
		data := c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key))
		if data == nil {
			return ErrKeyNotFound
		}
		_ = c.db.Del(t, c.EncodeKey(KEY_TYPE_HASH, key))
		slcRet := c.db.Scan(c.HashEncodeKey(key, []byte{}))
		for _, v := range slcRet {
			_ = c.db.Del(t, c.HashEncodeKey(key, v.V0))
		}
		return nil
	})
}

func (c *RedisCommand) HSet(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		expire, v := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		hLen := uint32(0)
		if len(v) > 0 {
			hLen = binary.LittleEndian.Uint32(v)
		}
		if expire {
			hLen = uint32(0)
		}
		add := uint32(0)
		for i := 0; i < len(args) && i+1 < len(args); i += 2 {
			ret := c.db.Get(t, c.HashEncodeKey(key, args[i]))
			if ret == nil {
				add++
			}
			err = c.db.Put(t, c.HashEncodeKey(key, args[i]), args[i+1])
			if err != nil {
				return err
			}
		}
		if add > 0 {
			meta := make([]byte, 4)
			binary.LittleEndian.PutUint32(meta, hLen+add)
			err = c.db.Put(t, c.EncodeKey(KEY_TYPE_HASH, key), c.EncodeValue(meta, 0))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *RedisCommand) HGet(key []byte, field ...[]byte) (ret [][]byte, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, v := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			c.HashDel(key)
			return ErrKeyNotFound
		}
		if v == nil {
			return ErrKeyNotFound
		}

		var res []byte
		for _, v := range field {
			res = c.db.Get(t, c.HashEncodeKey(key, v))
			if res == nil {
				return ErrKeyNotFound
			}
			ret = append(ret, res)
		}
		return nil
	})
	return
}

func (c *RedisCommand) HLen(key []byte) (ret uint32, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, value := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			c.HashDel(key)
			return ErrKeyNotFound
		}
		ret = binary.LittleEndian.Uint32(value)
		return nil
	})
	return
}

func (c *RedisCommand) HDel(key []byte, args ...[]byte) (ret uint32, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			c.HashDel(key)
			return ErrKeyNotFound
		}

		for _, v := range args {
			err = c.db.Del(t, c.HashEncodeKey(key, v))
			if err != nil {
				return err
			}
			ret++
		}
		return nil
	})
	return
}

func (c *RedisCommand) HGetAll(key []byte) (ret []*store.Pair, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			c.HashDel(key)
			return ErrKeyNotFound
		}

		var field []byte
		slcRet := c.db.Scan(c.HashEncodePrefix(key))
		for _, v := range slcRet {
			field = c.HashDecodeKey(v.V0)
			ret = append(ret, &store.Pair{field, v.V1})
		}
		return nil
	})
	return
}

func (c *RedisCommand) HExists(key, field []byte) (ret int, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			c.HashDel(key)
			return ErrKeyNotFound
		}

		value := c.db.Get(t, c.HashEncodeKey(key, field))
		if value != nil {
			ret = 1
		}
		return nil
	})
	return
}

func (c *RedisCommand) HKeys(key []byte) (ret [][]byte) {
	err := c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			return ErrKeyNotFound
		}

		var field []byte
		slcRet := c.db.Scan(c.HashEncodePrefix(key))
		for _, v := range slcRet {
			field = c.HashDecodeKey(v.V0)
			ret = append(ret, field)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) HTtl(key []byte) (ret [][]byte) {
	err := c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_HASH, key)))
		if expire {
			return ErrKeyNotFound
		}

		var field []byte
		slcRet := c.db.Scan(c.HashEncodeKey(key, []byte{}))
		for _, v := range slcRet {
			field = c.HashDecodeKey(v.V0)
			ret = append(ret, field)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return
}
