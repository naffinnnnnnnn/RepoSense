# MCP 契约

当前能力契约版本为 `1.0`。官方 MCP Go SDK 根据
`internal/application/mcp` 的强类型输入/输出在运行时生成并发布 JSON Schema，
传输适配器只能调用该应用门面。

工具：`search_code`、`get_symbol`、`find_call_chain`、`get_wiki_page`、
`ask_repository`。

Resource Template：

```text
reposense://wiki/{repository_id}/{snapshot_id}/{slug}
```

完整字段、限制、错误码和启动方式见 [`docs/MCP_SERVER.md`](../../docs/MCP_SERVER.md)。
