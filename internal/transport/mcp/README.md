# MCP 传输层

此目录使用官方 MCP Go SDK 完成协议适配和 stdio 传输。协议层只负责 Tool、
Resource Template 与领域 DTO 的映射；鉴权、限流和审计由 MCP 应用门面执行。

所有诊断日志写入标准错误，标准输出仅用于 MCP 消息。协议契约由强类型输入/
输出生成 JSON Schema，并通过官方 SDK 内存客户端执行发现、调用和 Resource
读取契约测试。
