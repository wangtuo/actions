package tsdb

import (
	"context"
	"path/filepath"
	"testing"
)

func mustOpen(t *testing.T, dir string, threshold int) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir, FlushThreshold: threshold})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func TestAppendAndQueryHead(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 0)
	defer db.Close()

	l := map[string]string{"__name__": "cpu", "host": "a"}
	for i := int64(0); i < 5; i++ {
		if err := db.AppendMap(l, i, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	res, err := db.Query(QueryRequest{Matchers: map[string]string{"__name__": "cpu"}})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("want 1 series, got %d", len(res))
	}
	if got := len(res[0].Samples); got != 5 {
		t.Fatalf("want 5 samples, got %d", got)
	}
	for i, sm := range res[0].Samples {
		if sm.T != int64(i) || sm.V != float64(i) {
			t.Fatalf("sample[%d]=%+v", i, sm)
		}
	}
}

func TestQueryTimeRangeAndMatcher(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 0)
	defer db.Close()

	_ = db.AppendMap(map[string]string{"__name__": "cpu", "host": "a"}, 10, 1)
	_ = db.AppendMap(map[string]string{"__name__": "cpu", "host": "a"}, 20, 2)
	_ = db.AppendMap(map[string]string{"__name__": "cpu", "host": "b"}, 15, 9)
	_ = db.AppendMap(map[string]string{"__name__": "mem", "host": "a"}, 15, 42)

	res, err := db.Query(QueryRequest{
		Matchers: map[string]string{"__name__": "cpu"},
		Start:    12, End: 20,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("want 2 series, got %d: %+v", len(res), res)
	}
	// host=a 应剩 1 个样本 (T=20)，host=b 应剩 1 个样本 (T=15)
	got := map[string]int{}
	for _, s := range res {
		got[s.Labels.Map()["host"]] = len(s.Samples)
	}
	if got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestAggregations(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 0)
	defer db.Close()

	l := map[string]string{"__name__": "req"}
	values := []float64{1, 2, 3, 4, 5}
	for i, v := range values {
		_ = db.AppendMap(l, int64(i), v)
	}
	cases := []struct {
		agg  Aggregation
		want float64
	}{
		{AggSum, 15}, {AggAvg, 3}, {AggCount, 5}, {AggMin, 1}, {AggMax, 5},
	}
	for _, c := range cases {
		res, err := db.Query(QueryRequest{Matchers: l, Agg: c.agg})
		if err != nil {
			t.Fatalf("%s: %v", c.agg, err)
		}
		if len(res) != 1 || res[0].Aggregate == nil {
			t.Fatalf("%s: bad result %+v", c.agg, res)
		}
		if *res[0].Aggregate != c.want {
			t.Fatalf("%s: want %v got %v", c.agg, c.want, *res[0].Aggregate)
		}
	}
}

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 0)
	l := map[string]string{"__name__": "cpu"}
	for i := int64(0); i < 3; i++ {
		if err := db.AppendMap(l, i, float64(i*10)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开：不 flush，纯靠 WAL 恢复
	db2 := mustOpen(t, dir, 0)
	defer db2.Close()
	res, err := db2.Query(QueryRequest{Matchers: l})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 || len(res[0].Samples) != 3 {
		t.Fatalf("replay lost samples: %+v", res)
	}
}

func TestFlushAndReload(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 0)
	l := map[string]string{"__name__": "cpu", "host": "a"}
	for i := int64(0); i < 4; i++ {
		_ = db.AppendMap(l, i, float64(i))
	}
	if err := db.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// flush 后 head 样本应清空，但 block 里应有
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 检查确实生成了 block 文件
	matches, _ := filepath.Glob(filepath.Join(dir, blockPrefix+"*"+blockSuffix))
	if len(matches) != 1 {
		t.Fatalf("want 1 block file, got %d", len(matches))
	}

	db2 := mustOpen(t, dir, 0)
	defer db2.Close()
	res, err := db2.Query(QueryRequest{Matchers: l})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 || len(res[0].Samples) != 4 {
		t.Fatalf("reload lost samples: %+v", res)
	}
}

func TestFlushThresholdAndCrossBlockQuery(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, 3) // 每 3 样本 flush
	defer db.Close()

	l := map[string]string{"__name__": "cpu"}
	for i := int64(0); i < 7; i++ {
		if err := db.AppendMap(l, i, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// 期望产生 2 个 block（3 + 3），head 剩 1 个
	blocks, _ := filepath.Glob(filepath.Join(dir, blockPrefix+"*"+blockSuffix))
	if len(blocks) != 2 {
		t.Fatalf("want 2 blocks, got %d", len(blocks))
	}
	res, err := db.Query(QueryRequest{Matchers: l})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 || len(res[0].Samples) != 7 {
		t.Fatalf("cross-block query missing samples: %+v", res)
	}
	for i, sm := range res[0].Samples {
		if sm.T != int64(i) {
			t.Fatalf("out of order at %d: %+v", i, sm)
		}
	}
}
