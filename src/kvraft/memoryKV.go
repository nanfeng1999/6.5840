package kvraft

type MemoryKV struct {
	Data map[string]string
}

func (mkv *MemoryKV) put(key string, value string) {
	mkv.Data[key] = value
}

func (mkv *MemoryKV) appendVal(key string, value string) {
	originVal := mkv.Data[key]
	mkv.Data[key] = originVal + value
}

func (mkv *MemoryKV) hasKey(key string) bool {
	_, ok := mkv.Data[key]
	return ok
}

func (mkv *MemoryKV) get(key string) string {
	return mkv.Data[key]
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		make(map[string]string),
	}
}
