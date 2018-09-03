package storage

const (
	ReplicationFactor = 3
	MinRedundancy     = 2
)

type ServiceAddr string
type RecordID uint32

func (RecordID) BinSize() int {
	return 4
}

func (addr ServiceAddr) BinSize() int {
	return len(addr)
}
