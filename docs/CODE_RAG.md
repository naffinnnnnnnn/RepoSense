# Code RAG 检索模块

## 边界与调用关系

Code RAG 将 Repository Parser 产出的 `CodeArtifact` 建成固定快照索引，并向
AI Wiki、Repository Agent 以及后续 MCP `search_code` 工具返回源码证据；它不
生成最终答案，也不会隐式读取最新快照。

```text
CodeArtifact[] → Index Revision → SYMBOL / KEYWORD / SEMANTIC recall
                                    ↓
                              GRAPH neighbor expansion
                                    ↓
                         weighted fusion → rerank
                                    ↓
                    Hits + ContextBundle + Diagnostics
```

所有索引与查询必须显式携带 `tenant_id + repository_id + snapshot_id`。内存适配器
和后续数据库适配器都必须使用完整作用域键，防止跨租户、跨仓库或跨版本串读。

## 输入输出

索引入口保持稳定端口：

```go
Index(ctx, scope, artifacts) (rag.IndexRevision, error)
```

检索入口接受查询、策略、过滤器和 `top_k`。策略名称为 `SYMBOL`、`KEYWORD`、
`SEMANTIC`、`GRAPH`，并兼容边界别名 `BM25` 和 `VECTOR`。过滤器支持语言、
制品类型、仓库相对路径前缀和制品 ID。

`EvidenceBundle` 包含：

- `hits`：总分、四路分项分数、命中原因和精确 `SourceRef`；
- `context_bundle`：按排名组装并受字符预算限制的检索文本；
- `diagnostics`：索引/算法版本、策略命中数、耗时、警告和查询哈希；
- `artifact_ids/sources`：供现有 Wiki 与 Agent 使用的兼容视图。

## 默认算法与替换点

默认实现无需外部服务即可运行：符号前缀匹配、BM25 风格词频评分、基于 token
和字符 n-gram 的确定性特征哈希向量，以及知识图谱一跳扩展。特征哈希是离线
基线，不等同于生产 embedding 模型。

生产优化通过以下端口接入：

- `Vectorizer`：替换为 Eino/模型供应商 embedding；
- `Reranker`：替换为 cross-encoder 或 LLM reranker；
- `RAGRepository`：替换为 PostgreSQL FTS + pgvector；
- `GraphStore`：复用 Neo4j 图查询实现。

每个索引修订记录完整算法标签，便于离线评测、灰度和回滚。

## 异常与可观测性

- 输入、索引缺失、索引失败、检索失败和持久化失败均有可判定领域错误码；
- Vectorizer/Reranker 故障返回可重试错误；Graph 故障按单源降级处理；
- `context.Context` 取消会沿向量化、图查询和存储调用传播；
- 索引和检索阶段记录耗时，指标覆盖文档数、向量数、请求数、命中数和幂等命中；
- 日志标签只包含作用域与 `trace_id`，不记录查询正文或源码全文。

## 测试范围

测试覆盖策略别名和边界校验、混合排序、过滤器、上下文截断、索引幂等、
租户/快照隔离、混合 commit 拒绝、向量供应商失败、图扩展/降级，以及真实
RAG Retriever 到 Repository Agent 的端到端证据传递。
