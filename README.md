# RepoSense

RepoSense 是一个代码知识平台。本仓库当前包含完整的模块化单体项目骨架，
以及可用于本地 Git 仓库的 Repository Parser、Code Knowledge Graph、Code RAG、
AI Wiki、Repository 智能问答 Agent 与可视化代码理解纵向功能切片。

## 快速开始

```powershell
go test ./...
go run ./cmd/worker parse --repo . --repository-id reposense --tenant-id local --ref HEAD
```

Worker 会将 JSON 格式的 `ParseResult` 写入标准输出。诊断日志写入标准错误，
且不会包含源代码内容或凭据。

契约和行为说明请参阅 `docs/repository-parser.md`、`docs/CODE_KNOWLEDGE_GRAPH.md`、
`docs/CODE_RAG.md`、`docs/AI_WIKI.md`、`docs/REPOSITORY_AGENT.md` 与
`docs/CODE_VISUALIZATION.md`。
