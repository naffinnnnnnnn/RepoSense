# Repository Agent 用例

`Service` 实现 `Guard → Planner → Retrieve → Evaluate → Synthesize → Finalize`：

- `Planner`、`Retriever`、`GraphStore`、`AnswerGenerator` 均通过接口替换；
- 检索循环最多五轮，默认两轮，单次结果量和图查询规模有硬上限；
- Retriever 与 Graph 允许单源降级，双源均故障时运行失败；
- 无有效引用时返回明确的 `insufficient_evidence`，不会绕过证据直接回答；
- 事件有严格递增序号并以 `COMPLETED` 或 `FAILED` 结束。
