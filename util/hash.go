package util

func BKDRHash(str []byte) uint32 {
	var seed uint32 = 131
	var hash uint32 = 0
	for i := 0; i < len(str); i++ {
		hash = hash*seed + uint32(str[i])
	}
	return hash & 0x7FFFFFFF
}