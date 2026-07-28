package tsdb

// Sample 是单个时间点的取值。ts 使用毫秒时间戳。
type Sample struct {
	T int64
	V float64
}

// Series 表示一条时间序列。
type Series struct {
	ID      uint64
	Labels  Labels
	Samples []Sample // 按 T 升序
}

// insertSample 保持有序插入；同 T 覆盖为最新 V（简化处理）。
func (s *Series) insertSample(sm Sample) {
	n := len(s.Samples)
	if n == 0 || s.Samples[n-1].T < sm.T {
		s.Samples = append(s.Samples, sm)
		return
	}
	if s.Samples[n-1].T == sm.T {
		s.Samples[n-1].V = sm.V
		return
	}
	// 罕见路径：乱序写入
	idx := lowerBound(s.Samples, sm.T)
	if idx < len(s.Samples) && s.Samples[idx].T == sm.T {
		s.Samples[idx].V = sm.V
		return
	}
	s.Samples = append(s.Samples, Sample{})
	copy(s.Samples[idx+1:], s.Samples[idx:])
	s.Samples[idx] = sm
}

func lowerBound(ss []Sample, t int64) int {
	lo, hi := 0, len(ss)
	for lo < hi {
		mid := (lo + hi) / 2
		if ss[mid].T < t {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
