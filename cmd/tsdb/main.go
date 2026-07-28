package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/example/actions-demo/pkg/tsdb"
)

// 用法：
//
//	tsdb -dir /tmp/db write cpu host=a value=0.5 [ts=<ms>]
//	tsdb -dir /tmp/db query __name__=cpu [host=a] [start=<ms>] [end=<ms>] [agg=sum|avg|count|min|max]
//	tsdb -dir /tmp/db flush
func main() {
	dir := flag.String("dir", "./tsdb-data", "TSDB 数据目录")
	flushEvery := flag.Int("flush-every", 0, "自动 flush 阈值（样本数），0 表示不自动")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	db, err := tsdb.Open(tsdb.Options{Dir: *dir, FlushThreshold: *flushEvery})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	switch args[0] {
	case "write":
		if err := cmdWrite(db, args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
	case "query":
		if err := cmdQuery(db, args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "query:", err)
			os.Exit(1)
		}
	case "flush":
		if err := db.Flush(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, "flush:", err)
			os.Exit(1)
		}
		fmt.Println("ok")
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  tsdb -dir <dir> write <metric> <k=v>... value=<float> [ts=<ms>]
  tsdb -dir <dir> query <k=v>... [start=<ms>] [end=<ms>] [agg=sum|avg|count|min|max]
  tsdb -dir <dir> flush`)
}

func parseKV(args []string) (labels map[string]string, extras map[string]string) {
	labels = map[string]string{}
	extras = map[string]string{}
	for _, a := range args {
		kv := strings.SplitN(a, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "value", "ts", "start", "end", "agg":
			extras[kv[0]] = kv[1]
		default:
			labels[kv[0]] = kv[1]
		}
	}
	return
}

func cmdWrite(db *tsdb.DB, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("write 需要 <metric> 和至少 value=<float>")
	}
	metric := args[0]
	labels, extras := parseKV(args[1:])
	labels["__name__"] = metric
	vs, ok := extras["value"]
	if !ok {
		return fmt.Errorf("缺少 value=<float>")
	}
	v, err := strconv.ParseFloat(vs, 64)
	if err != nil {
		return fmt.Errorf("value 非法: %w", err)
	}
	ts := time.Now().UnixMilli()
	if s, ok := extras["ts"]; ok {
		if p, err := strconv.ParseInt(s, 10, 64); err == nil {
			ts = p
		}
	}
	if err := db.AppendMap(labels, ts, v); err != nil {
		return err
	}
	fmt.Printf("ok ts=%d\n", ts)
	return nil
}

func cmdQuery(db *tsdb.DB, args []string) error {
	labels, extras := parseKV(args)
	req := tsdb.QueryRequest{Matchers: labels}
	if s, ok := extras["start"]; ok {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("start 非法: %w", err)
		}
		req.Start = v
	}
	if s, ok := extras["end"]; ok {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("end 非法: %w", err)
		}
		req.End = v
	}
	if a, ok := extras["agg"]; ok {
		req.Agg = tsdb.Aggregation(a)
	}
	res, err := db.Query(req)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}
