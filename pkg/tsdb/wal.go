package tsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WAL 记录格式（小端）：
//
//	1 字节 recordType
//	4 字节 payloadLen
//	payload
//	4 字节 CRC32 IEEE（对 recordType+payloadLen+payload）
//
// recordType:
//
//	1 = seriesDef   payload: varint id + labels(count + [name/value 长度前缀])
//	2 = sample      payload: id(uint64) + t(int64) + v(float64)
const (
	recSeriesDef byte = 1
	recSample    byte = 2

	walFileName = "wal.log"
)

// wal 负责追加写与重放。
type wal struct {
	dir  string
	f    *os.File
	buf  *bufio.Writer
	size int64
}

func openWAL(dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &wal{
		dir:  dir,
		f:    f,
		buf:  bufio.NewWriter(f),
		size: fi.Size(),
	}, nil
}

func (w *wal) Close() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

func (w *wal) Sync() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// truncate 清空 WAL（flush 完成后调用）。
func (w *wal) truncate() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.buf.Reset(w.f)
	w.size = 0
	return nil
}

func (w *wal) writeSeriesDef(id uint64, labels Labels) error {
	payload := encodeSeriesDef(id, labels)
	return w.writeRecord(recSeriesDef, payload)
}

func (w *wal) writeSample(id uint64, t int64, v float64) error {
	var payload [8 + 8 + 8]byte
	binary.LittleEndian.PutUint64(payload[0:8], id)
	binary.LittleEndian.PutUint64(payload[8:16], uint64(t))
	binary.LittleEndian.PutUint64(payload[16:24], f64bits(v))
	return w.writeRecord(recSample, payload[:])
}

func (w *wal) writeRecord(typ byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	crc := crc32IEEE(hdr[:], payload)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.buf.Write(crcBuf[:]); err != nil {
		return err
	}
	w.size += int64(len(hdr)) + int64(len(payload)) + 4
	return nil
}

// walRecord 是重放时给上层的回调数据。
type walRecord struct {
	typ    byte
	id     uint64
	labels Labels
	t      int64
	v      float64
}

// replayWAL 读取 wal.log 并回调每条有效记录。遇到损坏尾部时截断到最后有效位置。
func replayWAL(dir string, fn func(walRecord) error) error {
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var offset int64
	var hdr [5]byte
	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return truncateAt(f, offset)
		}
		typ := hdr[0]
		plen := binary.LittleEndian.Uint32(hdr[1:5])
		if plen > 64<<20 { // 单条 > 64MiB，视为损坏
			return truncateAt(f, offset)
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return truncateAt(f, offset)
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			return truncateAt(f, offset)
		}
		gotCRC := binary.LittleEndian.Uint32(crcBuf[:])
		if gotCRC != crc32IEEE(hdr[:], payload) {
			return truncateAt(f, offset)
		}

		rec, err := decodeRecord(typ, payload)
		if err != nil {
			return truncateAt(f, offset)
		}
		if err := fn(rec); err != nil {
			return err
		}
		offset += int64(len(hdr)) + int64(plen) + 4
	}
	return nil
}

func truncateAt(f *os.File, off int64) error {
	if err := f.Truncate(off); err != nil {
		return fmt.Errorf("truncate wal at %d: %w", off, err)
	}
	return nil
}

func decodeRecord(typ byte, payload []byte) (walRecord, error) {
	switch typ {
	case recSeriesDef:
		id, labels, err := decodeSeriesDef(payload)
		if err != nil {
			return walRecord{}, err
		}
		return walRecord{typ: typ, id: id, labels: labels}, nil
	case recSample:
		if len(payload) != 24 {
			return walRecord{}, errors.New("bad sample payload")
		}
		id := binary.LittleEndian.Uint64(payload[0:8])
		t := int64(binary.LittleEndian.Uint64(payload[8:16]))
		v := bitsToF64(binary.LittleEndian.Uint64(payload[16:24]))
		return walRecord{typ: typ, id: id, t: t, v: v}, nil
	default:
		return walRecord{}, fmt.Errorf("unknown record type %d", typ)
	}
}

func encodeSeriesDef(id uint64, labels Labels) []byte {
	// id(8) + count(4) + [nameLen(4)+name + valueLen(4)+value]*
	size := 8 + 4
	for _, l := range labels {
		size += 4 + len(l.Name) + 4 + len(l.Value)
	}
	buf := make([]byte, size)
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], id)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(labels)))
	off += 4
	for _, l := range labels {
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(l.Name)))
		off += 4
		copy(buf[off:], l.Name)
		off += len(l.Name)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(l.Value)))
		off += 4
		copy(buf[off:], l.Value)
		off += len(l.Value)
	}
	return buf
}

func decodeSeriesDef(payload []byte) (uint64, Labels, error) {
	if len(payload) < 12 {
		return 0, nil, errors.New("bad series-def payload")
	}
	id := binary.LittleEndian.Uint64(payload[0:8])
	count := binary.LittleEndian.Uint32(payload[8:12])
	off := 12
	labels := make(Labels, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(payload) {
			return 0, nil, errors.New("truncated label name len")
		}
		nl := int(binary.LittleEndian.Uint32(payload[off:]))
		off += 4
		if off+nl > len(payload) {
			return 0, nil, errors.New("truncated label name")
		}
		name := string(payload[off : off+nl])
		off += nl
		if off+4 > len(payload) {
			return 0, nil, errors.New("truncated label value len")
		}
		vl := int(binary.LittleEndian.Uint32(payload[off:]))
		off += 4
		if off+vl > len(payload) {
			return 0, nil, errors.New("truncated label value")
		}
		val := string(payload[off : off+vl])
		off += vl
		labels = append(labels, Label{Name: name, Value: val})
	}
	return id, labels, nil
}
