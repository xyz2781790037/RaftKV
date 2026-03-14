package badger

const MaxVLogFileSize = 64 * 1024 * 1024

type Options struct {
	Dir              string
	ValueLogFileSize int64
	SyncWrites       bool
}

func DefaultOptions(path string) Options {
	return Options{
		Dir:              path,
		ValueLogFileSize: MaxVLogFileSize,
		SyncWrites:       false,
	}
}
