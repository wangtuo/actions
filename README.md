# Go + GitHub Actions 全功能 Demo

一个可直接运行的最小 Go 项目，配套完整的 GitHub Actions 工作流集合，用于演示常见 CI/CD 场景。

## 目录结构

```
.
├── cmd/hello/          # 可执行程序入口
├── cmd/tsdb/           # 极简 TSDB CLI（写入 / 查询 / flush）
├── pkg/greeter/        # 库代码 + 单元测试
├── pkg/math/           # 单元测试 / benchmark / fuzz 演示
├── pkg/tsdb/           # 磁盘持久化 + WAL 的最小 TSDB 库
├── Dockerfile          # 多阶段构建 -> distroless
├── .goreleaser.yaml    # Release 打包配置
├── .golangci.yml       # 静态检查配置
└── .github/
    ├── dependabot.yml
    └── workflows/
        ├── ci.yml                    # 主 CI：lint + 多矩阵测试 + 构建 artifact
        ├── release.yml               # tag 触发 GoReleaser 发布
        ├── security.yml              # CodeQL + govulncheck（含定时）
        ├── docker.yml                # 多架构镜像 -> GHCR
        ├── reusable-go-test.yml      # 复用工作流（可被其他仓库调用）
        ├── nightly.yml               # 定时 + 手动触发 + 调用复用工作流
        └── issue-triage.yml          # issue/PR 自动欢迎与打标签
```

## 本地运行

```bash
go run ./cmd/hello -name Ark -a 3 -b 4
# Hello, Ark!
# 3 + 4 = 7

go test ./...
go test -bench=. -benchmem ./pkg/math
go test -fuzz=FuzzAdd -fuzztime=5s ./pkg/math
```

## TSDB Demo

一个最小可用的时序数据库：labels + 毫秒时间戳 + float64 sample，
数据写入 WAL 追加日志，达到阈值或显式调用 `Flush` 时落盘为不可变 block 文件，
重启后通过 WAL 重放恢复未落盘数据，查询自动合并 blocks + head。

作为库使用：

```go
db, _ := tsdb.Open(tsdb.Options{Dir: "./data", FlushThreshold: 10000})
defer db.Close()

_ = db.AppendMap(map[string]string{"__name__": "cpu", "host": "a"}, time.Now().UnixMilli(), 0.42)

res, _ := db.Query(tsdb.QueryRequest{
    Matchers: map[string]string{"__name__": "cpu"},
    Agg:      tsdb.AggAvg,
})
```

作为 CLI 使用：

```bash
DIR=/tmp/tsdb
go run ./cmd/tsdb -dir $DIR write cpu host=a value=0.5
go run ./cmd/tsdb -dir $DIR query __name__=cpu agg=avg
go run ./cmd/tsdb -dir $DIR flush
```

## Workflow 一览

| 文件 | 触发 | 演示能力 |
| --- | --- | --- |
| `ci.yml` | push / PR / workflow_dispatch | 并发控制、多 OS × 多 Go 版本矩阵、gofmt/vet/golangci-lint、race + coverage、Job Summary、artifact 上传、条件 benchmark |
| `release.yml` | 打 tag `v*.*.*` | GoReleaser 全平台归档 + GitHub Release + changelog |
| `security.yml` | push / PR / 每周定时 | CodeQL SARIF 上传、govulncheck |
| `docker.yml` | push / tag / 手动 | QEMU + buildx 多架构、GHCR 登录、metadata-action、GHA 缓存 |
| `reusable-go-test.yml` | `workflow_call` | 可被外部复用，暴露 `inputs` 和 `outputs` |
| `nightly.yml` | schedule / 手动 | cron、`workflow_dispatch` 富输入、调用复用工作流并读取输出 |
| `issue-triage.yml` | issues / pull_request_target | `actions/github-script` 用 REST API 评论/打标签 |

## 关键技巧速查

- **并发取消**：`concurrency: { group: ..., cancel-in-progress: true }`。
- **权限最小化**：每个 workflow 顶部显式声明 `permissions:`。
- **矩阵**：`strategy.matrix` + `include` 精细控制组合；`fail-fast: false` 避免一个失败阻断其他。
- **缓存**：`actions/setup-go@v5` 内置 `cache: true`，无需手写 `actions/cache`。
- **输出上下文**：`echo "key=val" >> "$GITHUB_OUTPUT"`；跨 job 用 `needs.<id>.outputs.<key>`。
- **Job Summary**：`echo "..." >> "$GITHUB_STEP_SUMMARY"` 直接渲染 Markdown 到运行页。
- **复用工作流**：`uses: ./.github/workflows/xxx.yml`，同仓库相对路径即可。
- **注入版本号**：`go build -ldflags "-X main.version=$SHA"`。

## 后续可扩展

- 接入 Codecov / Coveralls：在 `ci.yml` 里加 `codecov/codecov-action@v4`。
- 需要 signed release：在 `release.yml` 里加 cosign / SLSA provenance。
- 需要发布到 Homebrew：在 `.goreleaser.yaml` 里配 `brews:` 段。
