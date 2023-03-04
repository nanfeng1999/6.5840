package kvraft

type MemoryKV struct {
	kv map[string]string
}

func (mkv *MemoryKV) put(key string, value string) {
	mkv.kv[key] = value
}

func (mkv *MemoryKV) appendVal(key string, value string) {
	originVal := mkv.kv[key]
	mkv.kv[key] = originVal + value
}

func (mkv *MemoryKV) hasKey(key string) bool {
	_, ok := mkv.kv[key]
	return ok
}

func (mkv *MemoryKV) get(key string) string {
	return mkv.kv[key]
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		make(map[string]string),
	}
}
