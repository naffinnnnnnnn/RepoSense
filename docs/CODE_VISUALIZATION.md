# 可视化代码理解模块

## 模块定位

Visualization 是知识图谱的只读投影层。每次查询都固定
`tenant_id + repository_id + snapshot_id + graph_revision_id`，不会隐式读取最新版本，
也不会修改图实体。输出可直接转换为 Cytoscape.js elements，同时提供确定性坐标、源码
跳转映射和可选 Mermaid 定义。

主要代码：

- `internal/domain/visualization`：查询、视图、过滤器、投影、布局、诊断与领域错误。
- `internal/application/visualization`：图查询编排、视图约束、过滤、布局、导出、缓存和观测。
- `internal/adapters/memory/visualization_repository.go`：线程安全、租户隔离、带 TTL 的参考缓存。
- `api/openapi/reposense.yaml`：`POST .../visualizations` 契约。

## 支持的视图

| 视图 | 默认实体 | 默认关系 | 默认布局 |
|---|---|---|---|
| `REPOSITORY_MAP` | 全部 | 全部 | `GRID` |
| `MODULE_DEPENDENCY` | 模块、包、文件 | `IMPORTS`、`DEPENDS_ON` | `DAG` |
| `CLASS_DIAGRAM` | 类、接口 | `EXTENDS`、`IMPLEMENTS` | `DAG` |
| `CALL_GRAPH` | 函数、方法、外部符号 | `CALLS` | `DAG` |
| `DATA_FLOW` | 函数、方法、符号、配置 | `CALLS`、`DEPENDS_ON` | `DAG` |

当前 Code IR 尚无变量级 `READS/WRITES/FLOWS_TO` 关系，因此 `DATA_FLOW` 只展示真实已抽取
的调用与依赖证据，并在 `diagnostics.warnings` 明确降级能力；不会推断或伪造数据流。

客户端过滤器只能收窄视图的默认实体和关系集合。查询还支持 0–10 跳、方向、最小关系
置信度、语言和最多 5,000 节点限制。根可使用 `node_id` 或 `artifact_id`。

## 输出与交互

- `nodes` / `edges` 使用 `id/source/target/label/properties`，可直接映射 Cytoscape.js。
- `layout.positions` 为稳定坐标，支持 DAG、网格和径向布局。
- `source_links` 同时覆盖节点与有证据的边，包含提交、路径、符号和闭区间行号。
- `mermaid` 按需生成，标签经过转义，适合嵌入 Wiki。
- `diagnostics` 暴露图访问量、输入/输出规模、截断、缓存命中、总耗时、布局耗时和告警。

投影缓存键由规范化查询生成，排除仅用于链路关联的 `trace_id`，但始终包含租户、仓库、
快照和图修订。缓存返回防御性拷贝，过期条目不会被读取。

## 错误与扩展点

领域错误区分无效输入、图不存在、修订漂移、投影失败和缓存失败，并标注是否可重试。
所有长循环尊重 `context.Context` 取消。服务记录 `visualization_project` 阶段以及缓存命中、
节点数和边数指标，且日志标签不包含源码正文。

`ports.LayoutEngine` 是布局、聚类、边路由和大图优化的算法边界；
`ports.VisualizationRepository` 可由 Redis/PostgreSQL 适配器替换。后续加入社区发现、图裁剪、
边聚合或 WebGL 布局时，无需改变领域 DTO 和 API。

## 运行测试

```powershell
$env:GOCACHE="$PWD/.gocache"
$env:GOTMPDIR="$PWD/.gotmp"
go test ./...
go test -race ./internal/application/visualization ./internal/adapters/memory
go vet ./...
```

测试覆盖输入边界、默认值、循环图布局、取消、过滤、源码链接、Mermaid 转义、修订漂移、
数据流降级告警、缓存隔离/过期/防御性拷贝，以及真实 Knowledge Graph 到调用图投影的联调。
