package tsdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
)

// Options 控制 DB 的持久化行为。
type Options struct {
	// Dir 是所有数据文件的根目录。
	Dir string
	// FlushThreshold 触发 head -> block 落盘的样本条数阈值（含）。
	// <=0 表示不自动触发，只能显式 Flush。
	FlushThreshold int
}

// DB 是嵌入式 TSDB 的入口，读写线程安全。
type DB struct {
	opts Options

	mu          sync.RWMutex
	seriesByID  map[uint64]*Series
	seriesByKey map[string]*Series // 以 labels 规范化字符串为 key，用于快速判重
	numSamples  int

	wal *wal

	blocks []blockMeta
}

// Open 打开或初始化一个 DB。会创建目录、重放 WAL、加载 block 元数据。
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("tsdb: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		opts:        opts,
		seriesByID:  make(map[uint64]*Series),
		seriesByKey: make(map[string]*Series),
	}
	if err := db.loadBlocks(); err != nil {
		return nil, err
	}
	w, err := openWAL(opts.Dir)
	if err != nil {
		return nil, err
	}
	db.wal = w
	// 重放 WAL 到 head
	if err := replayWAL(opts.Dir, db.applyWALRecord); err != nil {
		return nil, fmt.Errorf("wal replay: %w", err)
	}
	return db, nil
}

func (db *DB) loadBlocks() error {
	paths, err := listBlocks(db.opts.Dir)
	if err != nil {
		return err
	}
	for _, p := range paths {
		_, minT, maxT, err := readBlock(p)
		if err != nil {
			return fmt.Errorf("read block %s: %w", p, err)
		}
		db.blocks = append(db.blocks, blockMeta{path: p, minT: minT, maxT: maxT})
	}
	return nil
}

func (db *DB) applyWALRecord(rec walRecord) error {
	switch rec.typ {
	case recSeriesDef:
		if _, ok := db.seriesByID[rec.id]; !ok {
			s := &Series{ID: rec.id, Labels: rec.labels}
			db.seriesByID[rec.id] = s
			db.seriesByKey[rec.labels.String()] = s
		}
	case recSample:
		s, ok := db.seriesByID[rec.id]
		if !ok {
			// WAL 中 sample 在 def 前，视为损坏，跳过
			return nil
		}
		s.insertSample(Sample{T: rec.t, V: rec.v})
		db.numSamples++
	}
	return nil
}

// Close 刷新并关闭 WAL。若配置了 FlushThreshold 且 head 有数据，不会强制 flush；
// 若希望关闭时强制落盘，请先调用 Flush。
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal != nil {
		return db.wal.Close()
	}
	return nil
}

// Append 写入一个样本。ts 是毫秒时间戳。相同 labels 视为同一 series。
func (db *DB) Append(labels Labels, ts int64, v float64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	key := labels.String()
	s, ok := db.seriesByKey[key]
	if !ok {
		id := labels.Hash()
		// 极小概率哈希冲突：递增直到未占用
		for {
			existing, exists := db.seriesByID[id]
			if !exists {
				break
			}
			if existing.Labels.String() == key {
				s = existing
				break
			}
			id++
		}
		if s == nil {
			s = &Series{ID: id, Labels: labels}
			db.seriesByID[id] = s
			db.seriesByKey[key] = s
			if err := db.wal.writeSeriesDef(id, labels); err != nil {
				return err
			}
		}
	}
	if err := db.wal.writeSample(s.ID, ts, v); err != nil {
		return err
	}
	s.insertSample(Sample{T: ts, V: v})
	db.numSamples++

	if db.opts.FlushThreshold > 0 && db.numSamples >= db.opts.FlushThreshold {
		return db.flushLocked()
	}
	return nil
}

// AppendMap 是 Append 的便捷入口。
func (db *DB) AppendMap(m map[string]string, ts int64, v float64) error {
	return db.Append(NewLabels(m), ts, v)
}

// Flush 强制把 head 中的数据落盘为新 block，成功后清空 WAL。
func (db *DB) Flush(_ context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if db.numSamples == 0 {
		return nil
	}
	series := make([]*Series, 0, len(db.seriesByID))
	for _, s := range db.seriesByID {
		if len(s.Samples) > 0 {
			series = append(series, s)
		}
	}
	meta, err := writeBlock(db.opts.Dir, series)
	if err != nil {
		return err
	}
	if meta != nil {
		db.blocks = append(db.blocks, *meta)
	}
	// 清空 head 样本，但保留 series 定义（下次写入无需再次分配 id）
	for _, s := range db.seriesByID {
		s.Samples = nil
	}
	db.numSamples = 0
	// 清空 WAL：需要保留 series 定义，否则重启后新写样本会找不到 series。
	// 简化处理：Truncate 后重新写入所有 series def。
	if err := db.wal.truncate(); err != nil {
		return err
	}
	for _, s := range db.seriesByID {
		if err := db.wal.writeSeriesDef(s.ID, s.Labels); err != nil {
			return err
		}
	}
	return db.wal.Sync()
}

// QueryRequest 描述一次查询。
type QueryRequest struct {
	Matchers map[string]string // label 精确匹配；空表示全部
	Start    int64             // 毫秒，含
	End      int64             // 毫秒，含；0 表示不限
	Agg      Aggregation       // AggNone 时返回原始样本
}

// Aggregation 聚合方式。
type Aggregation string

const (
	AggNone  Aggregation = ""
	AggSum   Aggregation = "sum"
	AggAvg   Aggregation = "avg"
	AggCount Aggregation = "count"
	AggMin   Aggregation = "min"
	AggMax   Aggregation = "max"
)

// QueryResultSeries 是查询结果里的一条 series。
type QueryResultSeries struct {
	Labels    Labels
	Samples   []Sample // 当 Agg != AggNone 时长度为 0
	Aggregate *float64 // 当 Agg != AggNone 时非 nil
}

// Query 执行一次查询，合并 blocks + head。
func (db *DB) Query(req QueryRequest) ([]QueryResultSeries, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if req.End == 0 {
		req.End = int64(1<<62 - 1)
	}

	// merged[id] -> samples
	merged := map[uint64]*QueryResultSeries{}

	visitSample := func(s *Series, sm Sample) {
		if sm.T < req.Start || sm.T > req.End {
			return
		}
		if !s.Labels.Matches(req.Matchers) {
			return
		}
		r, ok := merged[s.ID]
		if !ok {
			r = &QueryResultSeries{Labels: s.Labels}
			merged[s.ID] = r
		}
		r.Samples = append(r.Samples, sm)
	}

	// 1. 扫 blocks（可通过 minT/maxT 剪枝）
	for _, bm := range db.blocks {
		if bm.maxT < req.Start || bm.minT > req.End {
			continue
		}
		series, _, _, err := readBlock(bm.path)
		if err != nil {
			return nil, err
		}
		for _, s := range series {
			if !s.Labels.Matches(req.Matchers) {
				continue
			}
			for _, sm := range s.Samples {
				visitSample(s, sm)
			}
		}
	}

	// 2. 扫 head
	for _, s := range db.seriesByID {
		if !s.Labels.Matches(req.Matchers) {
			continue
		}
		for _, sm := range s.Samples {
			visitSample(s, sm)
		}
	}

	// 汇总：按 labels 字符串排序，样本按 T 升序去重
	out := make([]QueryResultSeries, 0, len(merged))
	for _, r := range merged {
		sort.Slice(r.Samples, func(i, j int) bool { return r.Samples[i].T < r.Samples[j].T })
		r.Samples = dedupSamples(r.Samples)
		if req.Agg != AggNone {
			agg := computeAgg(req.Agg, r.Samples)
			r.Aggregate = &agg
			r.Samples = nil
		}
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.String() < out[j].Labels.String() })
	return out, nil
}

func dedupSamples(ss []Sample) []Sample {
	if len(ss) < 2 {
		return ss
	}
	out := ss[:1]
	for _, sm := range ss[1:] {
		if sm.T == out[len(out)-1].T {
			out[len(out)-1] = sm // 覆盖
			continue
		}
		out = append(out, sm)
	}
	return out
}

func computeAgg(a Aggregation, ss []Sample) float64 {
	if len(ss) == 0 {
		return 0
	}
	switch a {
	case AggCount:
		return float64(len(ss))
	case AggSum:
		var s float64
		for _, x := range ss {
			s += x.V
		}
		return s
	case AggAvg:
		var s float64
		for _, x := range ss {
			s += x.V
		}
		return s / float64(len(ss))
	case AggMin:
		m := ss[0].V
		for _, x := range ss[1:] {
			if x.V < m {
				m = x.V
			}
		}
		return m
	case AggMax:
		m := ss[0].V
		for _, x := range ss[1:] {
			if x.V > m {
				m = x.V
			}
		}
		return m
	}
	return 0
}
