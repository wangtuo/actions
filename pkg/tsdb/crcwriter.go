package tsdb

import (
	"hash"
	"hash/crc32"
	"io"
)

// crcWriter 把写入 underlying 的字节喂给 CRC32 IEEE hasher。
type crcWriter struct {
	w io.Writer
	h hash.Hash32
}

func newCRCWriter(w io.Writer) *crcWriter {
	return &crcWriter{w: w, h: crc32.New(castagnoli)}
}

func (c *crcWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		_, _ = c.h.Write(p[:n])
	}
	return n, err
}

func (c *crcWriter) Sum32() uint32 { return c.h.Sum32() }
