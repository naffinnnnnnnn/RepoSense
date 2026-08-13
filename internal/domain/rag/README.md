# Code RAG 领域

本目录定义与存储技术无关的 Code RAG 契约：

- `IndexDocument` 记录代码制品、源码位置、可检索文本、符号词、图引用和向量引用；
- `IndexRevision` 是按 `tenant + repository + snapshot` 隔离的不可变发布单元；
- `RetrievalRequest` 支持符号、关键词、语义和图检索，以及语言、类型、路径和制品过滤；
- `EvidenceBundle` 同时提供结构化命中、受预算约束的上下文和脱敏诊断信息。

领域层不依赖 Eino、数据库、向量模型或图数据库。查询正文不会进入诊断数据，
只记录 `sha256` 哈希。
