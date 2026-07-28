package tsdb

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strings"
)

// Labels 是 series 的标签集合，语义上等价于 map[string]string，
// 但对外暴露有序切片，便于稳定序列化和哈希。
type Labels []Label

type Label struct {
	Name, Value string
}

// NewLabels 将 map 规范化为按 name 排序的 Labels。
func NewLabels(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, Label{Name: k, Value: v})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

// Map 返回 labels 的 map 视图（副本）。
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// String 返回稳定的规范化字符串表示，如 {__name__="cpu",host="a"}。
func (ls Labels) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteByte('"')
		b.WriteString(l.Value)
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// Hash 稳定哈希，用作 series ID。用 sha256 前 8 字节，冲突几率对 demo 足够低。
func (ls Labels) Hash() uint64 {
	h := sha256.New()
	var buf [8]byte
	for _, l := range ls {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(l.Name)))
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(l.Name))
		binary.LittleEndian.PutUint64(buf[:], uint64(len(l.Value)))
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(l.Value))
	}
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

// Matches 判断 labels 是否包含 matcher 中的所有 kv（子集匹配，精确相等）。
func (ls Labels) Matches(matcher map[string]string) bool {
	if len(matcher) == 0 {
		return true
	}
	m := ls.Map()
	for k, v := range matcher {
		if got, ok := m[k]; !ok || got != v {
			return false
		}
	}
	return true
}
