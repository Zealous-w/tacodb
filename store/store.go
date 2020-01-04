package store

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

func NewDBStore(engine string) IStore {
	switch engine {
	case "boltdb":
		return NewBoltDB()
	case "leveldb":
		return NewLevelDB()
	}
	return nil
}
