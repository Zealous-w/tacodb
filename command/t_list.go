package command

import "encoding/binary"

const (
	LIST_LEFT_INDEX  uint64 = 9223372036854775807
	LIST_RIGHT_INDEX uint64 = 9223372036854775808
)

//list
//type-key_size-key-index, value
func (*RedisCommand) ListEncodeKey(key []byte, index uint64) []byte {
	ret := make([]byte, 1+4+len(key)+8)
	ret[0] = KEY_TYPE_LIST_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	binary.BigEndian.PutUint64(ret[1+4+len(key):], index)
	return ret
}

func (*RedisCommand) ListEncodePrefix(key []byte) []byte {
	ret := make([]byte, 1+4+len(key))
	ret[0] = KEY_TYPE_LIST_FIELD
	binary.LittleEndian.PutUint32(ret[1:], uint32(len(key)))
	copy(ret[1+4:], key)
	return ret
}

func (*RedisCommand) ListDecodeKey(key []byte) []byte {
	return nil
}

//len-index
type ListMeta struct {
	len        uint32
	leftIndex  uint64
	rightIndex uint64
}

func NewListMeta() *ListMeta {
	ret := &ListMeta{}
	ret.reset()
	return ret
}

func (l *ListMeta) reset() {
	l.len = 0
	l.leftIndex = LIST_LEFT_INDEX
	l.rightIndex = LIST_RIGHT_INDEX
}

func (*RedisCommand) ListEncodeMeta(meta *ListMeta) []byte {
	ret := make([]byte, 20)
	binary.LittleEndian.PutUint32(ret, meta.len)
	binary.LittleEndian.PutUint64(ret[4:], meta.leftIndex)
	binary.LittleEndian.PutUint64(ret[12:], meta.rightIndex)
	return ret
}

func (*RedisCommand) ListDecodeMeta(meta []byte) *ListMeta {
	if len(meta) < 20 {
		return nil
	}

	return &ListMeta{
		len:        binary.LittleEndian.Uint32(meta),
		leftIndex:  binary.LittleEndian.Uint64(meta[4:]),
		rightIndex: binary.LittleEndian.Uint64(meta[12:]),
	}
}

func (c *RedisCommand) ListDel(key []byte) error {
	return c.db.Transaction(func(t interface{}) error {
		data := c.db.Get(t, c.EncodeKey(KEY_TYPE_LIST, key))
		if data == nil {
			return ErrKeyNotFound
		}
		_ = c.db.Del(t, c.EncodeKey(KEY_TYPE_LIST, key))
		slcRet := c.db.Scan(c.ListEncodePrefix(key))
		for _, v := range slcRet {
			_ = c.db.Del(t, v.V0)
		}
		return nil
	})
}

func (c *RedisCommand) LPush(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		metaKey := c.EncodeKey(KEY_TYPE_LIST, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		metaInfo := c.ListDecodeMeta(meta)
		if metaInfo == nil {
			metaInfo = NewListMeta()
		}
		if expire {
			c.ListDel(key)
			metaInfo.reset()
		}

		var memberKey []byte
		for _, m := range args {
			memberKey = c.ListEncodeKey(key, metaInfo.leftIndex)
			err = c.db.Put(t, memberKey, m)
			if err != nil {
				return err
			}
			metaInfo.len++
			metaInfo.leftIndex--
		}
		return c.db.Put(t, metaKey, c.EncodeValue(c.ListEncodeMeta(metaInfo), 0))
	})
}

func (c *RedisCommand) LPop(key []byte) (ret []byte) {
	err := c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_LIST, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		if expire {
			c.ListDel(key)
			return ErrKeyNotFound
		}
		metaInfo := c.ListDecodeMeta(meta)
		popKey := c.ListEncodeKey(key, metaInfo.leftIndex+1)
		ret = c.db.Get(t, popKey)
		if ret == nil {
			return ErrKeyNotFound
		}
		metaInfo.leftIndex++
		metaInfo.len--
		err := c.db.Del(t, popKey)
		if err != nil {
			return err
		}
		return c.db.Put(t, metaKey, c.EncodeValue(c.ListEncodeMeta(metaInfo), 0))
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) LRange(key []byte, start, end int) (ret [][]byte) {
	_ = c.db.Transaction(func(t interface{}) error {
		ret = nil
		expire, meta := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_LIST, key)))
		if expire {
			c.ListDel(key)
			return ErrKeyNotFound
		}

		metaInfo := c.ListDecodeMeta(meta)
		if metaInfo == nil {
			return ErrKeyNotFound
		}

		lLen := int(metaInfo.rightIndex-metaInfo.leftIndex) - 1
		if lLen == 0 {
			return ErrKeyTypeError
		}
		rStart, rEnd := start, end

		if start < 0 {
			rStart = start + lLen
		}

		if end < 0 {
			rEnd = end + lLen
		}

		if rStart < 0 {
			rStart = 0
		}

		if rEnd < 0 {
			rEnd = 0
		}

		if rStart > rEnd {
			return ErrKeyTypeError
		}

		//interface range is [], so end + 1
		slc := c.db.Range(c.ListEncodeKey(key, metaInfo.leftIndex+uint64(rStart)+1), c.ListEncodeKey(key, metaInfo.leftIndex+uint64(rEnd)+1+1))
		for _, v := range slc {
			ret = append(ret, v.V1)
		}
		return nil
	})
	return
}

//[left, right)
func (c *RedisCommand) LTrim(key []byte, start, end int) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		expire, meta := c.DecodeValue(c.db.Get(t, c.EncodeKey(KEY_TYPE_LIST, key)))
		if expire {
			c.ListDel(key)
			return ErrKeyNotFound
		}

		metaInfo := c.ListDecodeMeta(meta)
		if metaInfo == nil {
			return ErrKeyNotFound
		}

		lLen := int(metaInfo.rightIndex-metaInfo.leftIndex) - 1
		rStart, rEnd := start, end

		if start < 0 {
			rStart = start + lLen
		}

		if end < 0 {
			rEnd = end + lLen
		}

		if rStart < 0 {
			rStart = 0
		}

		if rEnd < 0 {
			rEnd = 0
		}

		if rStart > rEnd {
			return ErrKeyTypeError
		}

		if rEnd >= lLen {
			rEnd = lLen - 1
		}

		lLeft, lRight := metaInfo.leftIndex+uint64(rStart), metaInfo.leftIndex+uint64(rEnd+1+1)
		lSlc := c.db.Range(c.ListEncodeKey(key, metaInfo.leftIndex), c.ListEncodeKey(key, lLeft))
		for _, v := range lSlc {
			err = c.db.Del(t, v.V0)
			if err != nil {
				return err
			}
		}
		rSlc := c.db.Range(c.ListEncodeKey(key, lRight), c.ListEncodeKey(key, metaInfo.rightIndex))
		for _, v := range rSlc {
			err = c.db.Del(t, v.V0)
			if err != nil {
				return err
			}
		}
		metaInfo.leftIndex = lLeft
		metaInfo.rightIndex = lRight
		metaInfo.len = uint32(rEnd-rStart) + 1

		return c.db.Put(t, c.EncodeKey(KEY_TYPE_LIST, key), c.EncodeValue(c.ListEncodeMeta(metaInfo), 0))
	})
}

func (c *RedisCommand) RPush(key []byte, args ...[]byte) error {
	return c.db.Transaction(func(t interface{}) error {
		var err error
		metaKey := c.EncodeKey(KEY_TYPE_LIST, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		metaInfo := c.ListDecodeMeta(meta)
		if metaInfo == nil {
			metaInfo = NewListMeta()
		}
		if expire {
			c.ListDel(key)
			metaInfo.reset()
		}

		var memberKey []byte
		for _, m := range args {
			memberKey = c.ListEncodeKey(key, metaInfo.rightIndex)
			err = c.db.Put(t, memberKey, m)
			if err != nil {
				return err
			}
			metaInfo.len++
			metaInfo.rightIndex++
		}
		return c.db.Put(t, metaKey, c.EncodeValue(c.ListEncodeMeta(metaInfo), 0))
	})
}

func (c *RedisCommand) RPop(key []byte) (ret []byte) {
	err := c.db.Transaction(func(t interface{}) error {
		metaKey := c.EncodeKey(KEY_TYPE_LIST, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		if expire {
			c.ListDel(key)
			return ErrKeyNotFound
		}
		metaInfo := c.ListDecodeMeta(meta)
		popKey := c.ListEncodeKey(key, metaInfo.leftIndex)
		ret = c.db.Get(t, popKey)
		if ret == nil {
			return ErrKeyNotFound
		}
		metaInfo.rightIndex--
		metaInfo.len--
		err := c.db.Del(t, popKey)
		if err != nil {
			return err
		}
		return c.db.Put(t, metaKey, c.EncodeValue(c.ListEncodeMeta(metaInfo), 0))
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) LLen(key []byte) (ret uint32) {
	_ = c.db.Transaction(func(t interface{}) error {
		ret = 0
		metaKey := c.EncodeKey(KEY_TYPE_LIST, key)
		expire, meta := c.DecodeValue(c.db.Get(t, metaKey))
		metaInfo := c.ListDecodeMeta(meta)
		if metaInfo == nil {
			return nil
		}
		if expire {
			c.ListDel(key)
			return nil
		}
		ret = uint32(metaInfo.rightIndex-metaInfo.leftIndex) - 1
		return nil
	})
	return
}
