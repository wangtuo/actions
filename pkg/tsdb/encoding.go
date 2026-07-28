package tsdb

import (
	"hash/crc32"
	"math"
)

var castagnoli = crc32.MakeTable(crc32.IEEE)

func crc32IEEE(parts ...[]byte) uint32 {
	h := crc32.New(castagnoli)
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	return h.Sum32()
}

func f64bits(v float64) uint64   { return math.Float64bits(v) }
func bitsToF64(u uint64) float64 { return math.Float64frombits(u) }
