package command

import "encoding/binary"

//set
//type-k_size-key-m_size-member
func (*RedisCommand) SetEncodeKey(key, member []byte) []byte {
	ret := make([]byte, 1+4+len(key)+4+len(member))
	ret[0] = KEY_TYPE_SET_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	binary.LittleEndian.PutUint32(ret[1+4+len(key):], uint32(len(member)))
	copy(ret[1+4+len(key)+4:], member)
	return ret
}

func (*RedisCommand) SetEncodePrefix(key []byte) []byte {
	ret := make([]byte, 1+4+len(key))
	ret[0] = KEY_TYPE_SET_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	return ret
}

func (*RedisCommand) SetDecodeKey(key []byte) []byte {
	return nil
}

func (c *RedisCommand) SetDel(key []byte) error {
	return c.db.Transaction(func(t interface{}) error {
		data := c.db.Get(t, c.EncodeKey(KEY_TYPE_SET, key))
		if data == nil {
			return ErrKeyNotFound
		}
		_ = c.db.Del(t, c.EncodeKey(KEY_TYPE_SET, key))
		slcRet := c.db.Scan(c.SetEncodePrefix(key))
		for _, v := range slcRet {
			_ = c.db.Del(t, c.SetEncodeKey(key, v.V0))
		}
		return nil
	})
}

func (c *RedisCommand) SAdd(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		sLen := uint32(0)
		metaKey := c.EncodeKey(KEY_TYPE_SET, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		if len(meta) > 0 {
			sLen = binary.LittleEndian.Uint32(meta)
		}
		if expire {
			c.SetDel(key)
			sLen = 0
		}

		count := uint32(0)

		var memberKey []byte
		for _, m := range args {
			memberKey = c.SetEncodeKey(key, m)
			exist := c.db.Get(t, memberKey)
			if exist != nil {
				continue
			}
			err = c.db.Put(t, memberKey, m)
			if err != nil {
				return err
			}
			count++
		}

		if count == 0 {
			return nil
		}

		sLen += uint32(count)
		metaData := make([]byte, 4)
		binary.LittleEndian.PutUint32(metaData, sLen)
		return c.db.Put(t, metaKey, c.EncodeValue(metaData, 0))
	})
}

func (c *RedisCommand) SRem(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		sLen := uint32(0)
		expire, meta := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_SET, key)))
		if len(meta) > 0 {
			sLen = binary.LittleEndian.Uint32(meta)
		}
		if expire {
			c.SetDel(key)
			return ErrKeyNotFound
		}

		for _, m := range args {
			err = c.db.Del(t, c.SetEncodeKey(key, m))
			if err != nil {
				return err
			}
		}

		sLen -= uint32(len(args))
		metaData := make([]byte, 4)
		binary.LittleEndian.PutUint32(metaData, sLen)
		return c.db.Put(t, c.EncodeKey(KEY_TYPE_SET, key), c.EncodeValue(metaData, 0))
	})
}

func (c *RedisCommand) SMembers(key []byte, args ...[]byte) (ret [][]byte, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, _ := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_SET, key)))
		if expire {
			c.SetDel(key)
			return ErrKeyNotFound
		}

		slc := c.db.Scan(c.SetEncodePrefix(key))
		for _, v := range slc {
			ret = append(ret, v.V1)
		}
		return nil
	})
	return
}

func (c *RedisCommand) SCard(key []byte) (ret uint32, err error) {
	err = c.db.Transaction(func(t interface{}) error {
		expire, meta := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_SET, key)))
		if expire {
			c.SetDel(key)
			return ErrKeyNotFound
		}

		if len(meta) > 0 {
			ret = binary.LittleEndian.Uint32(meta)
		}
		return nil
	})
	return
}
