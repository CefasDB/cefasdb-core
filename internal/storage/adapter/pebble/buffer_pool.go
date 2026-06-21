package pebble

import "sync"

// encodeBufPool reuses backing arrays for storage.EncodeItemAppend on
// the write hot path. Pebble's batch.Set copies the value into the
// batch's own buffer, so callers are free to return the slice to the
// pool immediately after Set returns.
//
// The pool stores *[]byte rather than []byte to avoid a per-Get heap
// allocation on the interface conversion (Go vet flag-suggested).
var encodeBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 512)
		return &buf
	},
}

func acquireEncodeBuf() *[]byte {
	return encodeBufPool.Get().(*[]byte)
}

func releaseEncodeBuf(buf *[]byte) {
	if buf == nil {
		return
	}
	const maxRetainedCap = 64 * 1024
	if cap(*buf) > maxRetainedCap {
		return // let it be GC'd; oversized items would bloat retained memory
	}
	*buf = (*buf)[:0]
	encodeBufPool.Put(buf)
}
