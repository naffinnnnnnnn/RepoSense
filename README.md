# RepoSense

RepoSense 是一个代码知识平台。本仓库当前包含完整的模块化单体项目骨架，
以及一个可用于本地 Git 仓库的 Repository Parser（仓库解析器）纵向功能切片。

## 快速开始

```powershell
go test ./...
go run ./cmd/worker parse --repo . --repository-id reposense --tenant-id local --ref HEAD
```

Worker 会将 JSON 格式的 `ParseResult` 写入标准输出。诊断日志写入标准错误，
且不会包含源代码内容或凭据。

契约和行为说明请参阅 `docs/repository-parser.md`。
