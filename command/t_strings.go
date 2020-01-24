package command

//string
func (c *RedisCommand) Set(key, value []byte, ttl uint32) error {
	db := c.DB(key)
	return db.Transaction(func(t interface{}) error {
		return db.Put(t, c.EncodeKey(KEY_TYPE_STRING, key), c.EncodeValue(value, ttl))
	})
}

func (c *RedisCommand) Get(key []byte) (ret []byte) {
	db := c.DB(key)
	err := db.Transaction(func(t interface{}) error {
		expire, value := c.DecodeValue(db.Get(t, c.EncodeKey(KEY_TYPE_STRING, key)))
		if expire {
			_ = c.Del(key)
			return ErrKeyNotFound
		}
		ret = value
		return nil
	})
	if err != nil {
		return nil
	}
	return
}

func (c *RedisCommand) Del(key []byte) int {
	var err error
	db := c.DB(key)
	err = db.Transaction(func(t interface{}) error {
		data := db.Get(t, c.EncodeKey(KEY_TYPE_STRING, key))
		if data == nil {
			return ErrKeyNotFound
		}
		return db.Del(t, c.EncodeKey(KEY_TYPE_STRING, key))
	})
	if err == nil {
		return 1
	}
	err = c.HashDel(key)
	if err == nil {
		return 1
	}
	err = c.ListDel(key)
	if err == nil {
		return 1
	}
	err = c.ZSetDel(key)
	if err == nil {
		return 1
	}
	err = c.SetDel(key)
	if err == nil {
		return 1
	}
	return 0
}
