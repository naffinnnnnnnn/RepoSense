# NATS 适配器

此目录实现 JetStream 发布者。EventID 作为 `Nats-Msg-Id`，从 Outbox 恢复发布时由 JetStream
进行消息去重；可信租户作用域使用 RepoSense 消息头传递，事件正文继续通过现有 JSON Schema。
设置 `REPOSENSE_TEST_NATS_URL` 可运行真实 JetStream 集成测试。
