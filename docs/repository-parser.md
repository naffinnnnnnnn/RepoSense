# Repository Parser（仓库解析器）

## 支持的输入

当前适配器接收本地 Git 工作树或 HTTPS/SSH 远程仓库、明确的 ref、租户与仓库隔离键、幂等键，
以及可选的仓库相对路径包含 Glob。Git 内容始终从解析后的 commit 中读取，
不会读取可能发生变化的工作树文件。

支持抽取以下代码结构：

- TypeScript/JavaScript：文件、类、接口、函数、方法、导入、调用、继承及接口实现。
- Python：文件、类、函数、方法、导入、调用及继承。
- Java：文件、类、接口、方法、导入、调用、继承及接口实现。
- Go：基于标准库 AST 抽取包、类型、接口、函数、方法、导入及调用。
- Rust：抽取模块、类型、Trait、函数、impl 方法、use 及基础调用。
- Markdown、文本及配置格式：文档或配置产物。

语言解析端口采用可替换设计。当前无外部依赖的结构解析适配器建立了 Code IR 契约；
后续可以引入 Tree-sitter 适配器，而无须修改应用层或领域层包。

## 增量行为

首次运行使用 `git ls-tree`，解析范围为 `FULL`。后续运行使用
`git diff --name-status --find-renames` 与最近一次成功快照进行比较，
解析范围为 `INCREMENTAL`。系统会解析新增、修改及重命名后的目标文件；
删除文件和重命名前的源文件通过 `deleted_paths` 输出。commit 未发生变化时，
系统返回成功的空增量结果，不会重新构建整个仓库。

## 安全与限制

- Git 命令使用参数向量调用；ref 和路径不会经过 Shell 展开。
- 源码路径必须为仓库相对路径，系统会拒绝目录穿越路径。
- 默认最多处理 100,000 个变更文件，单文件上限为 2 MiB。
- 二进制文件、无效 UTF-8 文件、超大文件及不支持的文件会被跳过，并记录原因指标。
- 日志只包含标识符、数量、状态及耗时，不包含源码或密钥。
- 解析失败时会保存脱敏后的失败状态及 `parse.failed.v1` 事件。

## 生产适配器边界

项目包含的内存存储使该纵向功能切片可以直接运行和测试。
PostgreSQL 迁移定义了生产环境中的原子持久化结构：Snapshot、ParseJob、
Code IR、幂等记录及 Outbox 事件必须在同一个事务中写入。
NATS 事件由 Outbox 分发器负责发布。事件 JSON 保持 v1 Schema，租户和仓库作用域通过
`RepoSense-Tenant-ID`、`RepoSense-Repository-ID` 和 `RepoSense-Snapshot-ID` 消息头传递，
下游仍会读取 PostgreSQL 并再次确认 Snapshot 为 `SUCCEEDED`。

## 生产进程与配置

- `go run ./cmd/api`：启动异步 HTTP API。
- `go run ./cmd/worker run`：启动带租约/心跳的解析 Worker、Outbox Dispatcher 和清理任务。
- `go run ./cmd/worker parse ...`：保留同步 CLI 兼容入口。

生产启动必须配置 `REPOSENSE_POSTGRES_DSN` 和 `REPOSENSE_NATS_URL`。资源与可靠性配置包括
`REPOSENSE_MAX_FILES`、`REPOSENSE_MAX_FILE_BYTES`、`REPOSENSE_GIT_OUTPUT_BYTES`、
`REPOSENSE_PARSE_TIMEOUT`、`REPOSENSE_FAILURE_CLEANUP_TIMEOUT`、`REPOSENSE_JOB_LEASE`、
`REPOSENSE_JOB_HEARTBEAT`、`REPOSENSE_OUTBOX_MAX_ATTEMPTS`、Outbox 退避、Workspace 缓存/保留期、
失败任务和已终结 Outbox 保留期。所有数值和 duration 必须为正数，心跳必须短于租约。
