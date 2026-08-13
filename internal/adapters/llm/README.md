# 模型适配器

`WikiGenerator` 已提供严格 JSON 输出、证据目录索引、Token 统计和 Prompt 注入隔离。Eino `ChatModel` 只需通过窄 `ChatModel` 接口接入；模型不能自行构造 `SourceRef`，只能引用应用提供的证据索引。

Code RAG 已通过 `Vectorizer` 与 `Reranker` 窄接口预留模型接入点，并提供可复现的
离线默认实现。生产 Embedding 和重排供应商只需实现对应接口，不进入领域层。
