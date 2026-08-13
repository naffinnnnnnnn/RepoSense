# Code RAG 用例

`Service` 实现完整的 `Index → Recall → Graph Expand → Fuse → Rerank → Context Build` 流程：

- 默认混合符号匹配、BM25 风格关键词、确定性特征哈希向量与图邻居召回；
- `Vectorizer`、`Reranker`、`GraphStore` 和 `RAGRepository` 均通过端口替换；
- 索引按固定快照原子发布，重复输入复用同一修订与事件；
- 图检索不可用时保留其他召回结果并返回诊断警告；
- 每个命中携带分项分数、命中原因和可验证 `SourceRef`；
- 上下文包受字符预算约束，避免下游 Agent/Wiki 无界消耗。

当前内存适配器用于单元测试和本地联调。生产适配器可将关键词、向量和图阶段
分别下推到 PostgreSQL FTS/pgvector 与 Neo4j，而无需修改应用服务或下游调用方。
