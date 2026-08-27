# PostgreSQL 适配器

此目录包含基于 pgx v5 的 Repository Parser 生产仓储。迁移和适配器共同提供仓库身份绑定、
原子幂等任务创建、租约领取/心跳/接管、线性 Snapshot CAS、Artifact 成功状态门禁、事务型
Outbox、同键失败重试和有界数据保留。设置 `REPOSENSE_TEST_POSTGRES_DSN` 可运行隔离 Schema
中的 PostgreSQL 集成测试。
