package tsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Block 文件格式（小端）：
//   magic       4 字节 "TSDB"
//   version     1 字节 = 1
//   minT/maxT   int64 * 2
//   seriesCount uint32
//   [ series ]* :
//        id(uint64)
//        labelsBlob（复用 wal 的 seriesDef encode）
//        sampleCount(uint32)
//        [ t(int64), v(float64) ]*
//   footer CRC32(全文)  uint32

const (
	blockMagic   = "TSDB"
	blockVersion = 1
	blockPrefix  = "block-"
	blockSuffix  = ".tsdb"
)

type blockMeta struct {
	path       string
	minT, maxT int64
}

// writeBlock 把一批 series 写为一个 block 文件，返回 block 元信息。
// 传入的 series 至少要有 1 个样本，否则跳过。
func writeBlock(dir string, series []*Series) (*blockMeta, error) {
	if len(series) == 0 {
		return nil, nil
	}
	sort.Slice(series, func(i, j int) bool { return series[i].ID < series[j].ID })

	// 过滤掉无样本 series，并求 minT/maxT
	filtered := series[:0]
	var minT, maxT int64
	minT, maxT = int64(1<<62), int64(-(1 << 62))
	for _, s := range series {
		if len(s.Samples) == 0 {
			continue
		}
		filtered = append(filtered, s)
		if s.Samples[0].T < minT {
			minT = s.Samples[0].T
		}
		if s.Samples[len(s.Samples)-1].T > maxT {
			maxT = s.Samples[len(s.Samples)-1].T
		}
	}
	if len(filtered) == 0 {
		return nil, nil
	}

	name := fmt.Sprintf("%s%d%s", blockPrefix, time.Now().UnixNano(), blockSuffix)
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(f)

	// 全文 CRC：所有写入 bw 的字节都要经过 hasher
	hasher := newCRCWriter(bw)

	// header
	if _, err := hasher.Write([]byte(blockMagic)); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := binary.Write(hasher, binary.LittleEndian, byte(blockVersion)); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := binary.Write(hasher, binary.LittleEndian, minT); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := binary.Write(hasher, binary.LittleEndian, maxT); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := binary.Write(hasher, binary.LittleEndian, uint32(len(filtered))); err != nil {
		return nil, closeErr(f, tmp, err)
	}

	for _, s := range filtered {
		labelsBlob := encodeSeriesDef(s.ID, s.Labels) // 内含 id + labels
		if err := binary.Write(hasher, binary.LittleEndian, uint32(len(labelsBlob))); err != nil {
			return nil, closeErr(f, tmp, err)
		}
		if _, err := hasher.Write(labelsBlob); err != nil {
			return nil, closeErr(f, tmp, err)
		}
		if err := binary.Write(hasher, binary.LittleEndian, uint32(len(s.Samples))); err != nil {
			return nil, closeErr(f, tmp, err)
		}
		for _, sm := range s.Samples {
			if err := binary.Write(hasher, binary.LittleEndian, sm.T); err != nil {
				return nil, closeErr(f, tmp, err)
			}
			if err := binary.Write(hasher, binary.LittleEndian, f64bits(sm.V)); err != nil {
				return nil, closeErr(f, tmp, err)
			}
		}
	}

	// footer：直接写到底层 bw，不参与 CRC
	crc := hasher.Sum32()
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := bw.Write(crcBuf[:]); err != nil {
		return nil, closeErr(f, tmp, err)
	}

	if err := bw.Flush(); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := f.Sync(); err != nil {
		return nil, closeErr(f, tmp, err)
	}
	if err := f.Close(); err != nil {
		return nil, os.Remove(tmp)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	return &blockMeta{path: path, minT: minT, maxT: maxT}, nil
}

func closeErr(f *os.File, tmp string, err error) error {
	_ = f.Close()
	_ = os.Remove(tmp)
	return err
}

// listBlocks 扫描目录，按文件名（含时间戳）升序返回。
func listBlocks(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, blockPrefix) && strings.HasSuffix(n, blockSuffix) {
			paths = append(paths, filepath.Join(dir, n))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// readBlock 读整个 block 到内存 series 切片。样本按 T 升序。
func readBlock(path string) ([]*Series, int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(buf) < 4 {
		return nil, 0, 0, errors.New("block too small")
	}
	// 校验 CRC
	body := buf[:len(buf)-4]
	gotCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if gotCRC != crc32IEEE(body) {
		return nil, 0, 0, errors.New("block crc mismatch")
	}

	off := 0
	if string(body[off:off+4]) != blockMagic {
		return nil, 0, 0, errors.New("bad block magic")
	}
	off += 4
	if body[off] != blockVersion {
		return nil, 0, 0, fmt.Errorf("unsupported block version %d", body[off])
	}
	off++
	minT := int64(binary.LittleEndian.Uint64(body[off:]))
	off += 8
	maxT := int64(binary.LittleEndian.Uint64(body[off:]))
	off += 8
	seriesCount := binary.LittleEndian.Uint32(body[off:])
	off += 4

	series := make([]*Series, 0, seriesCount)
	for i := uint32(0); i < seriesCount; i++ {
		if off+4 > len(body) {
			return nil, 0, 0, errors.New("truncated labels blob len")
		}
		blobLen := int(binary.LittleEndian.Uint32(body[off:]))
		off += 4
		if off+blobLen > len(body) {
			return nil, 0, 0, errors.New("truncated labels blob")
		}
		id, labels, err := decodeSeriesDef(body[off : off+blobLen])
		if err != nil {
			return nil, 0, 0, err
		}
		off += blobLen

		if off+4 > len(body) {
			return nil, 0, 0, errors.New("truncated sample count")
		}
		sc := int(binary.LittleEndian.Uint32(body[off:]))
		off += 4
		samples := make([]Sample, sc)
		for j := 0; j < sc; j++ {
			if off+16 > len(body) {
				return nil, 0, 0, errors.New("truncated sample")
			}
			t := int64(binary.LittleEndian.Uint64(body[off:]))
			off += 8
			v := bitsToF64(binary.LittleEndian.Uint64(body[off:]))
			off += 8
			samples[j] = Sample{T: t, V: v}
		}
		series = append(series, &Series{ID: id, Labels: labels, Samples: samples})
	}
	return series, minT, maxT, nil
}
