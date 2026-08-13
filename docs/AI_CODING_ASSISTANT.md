# AI Coding Assistant

## MVP 能力

Assistant 支持代码解释、重构建议与补丁草案。输出包含摘要、解释、逐文件 unified diff、测试计划、风险等级和可定位源码引用。系统不会自动提交代码；任何工作区修改都需要具备 `repo:write` 的人工审批。

## 调用流程

```text
CodingCommand
  -> Scope/权限/快照校验
  -> Code RAG（固定 snapshot_id）
  -> ProposalGenerator
  -> 引用、diff、大小和风险校验
  -> ChangeProposal(AWAITING_APPROVAL)
  -> Approval(repo:write)
  -> 基础 commit + 文件哈希校验
  -> git apply --check
  -> 原子应用到工作区
  -> proposal.applied.v1
```

`EXPLAIN` 不允许返回文件变化；`REFACTOR` 和 `PATCH` 至少包含一个文件变化。模型只能用证据目录中的引用索引，无法添加未知引用。每个文件变化只允许修改其声明路径，拒绝二进制、多文件注入、重命名和隐式新增/删除。

## 扩展点

- `ProposalGenerator`：可接入 Eino/任意 ChatModel、规则生成器或评测 Fake。
- `Retriever`：复用 Code RAG，后续可优化检索、重排和上下文预算。
- `AssistantRepository`：内存实现用于测试/本地开发，生产可实现 PostgreSQL 事务与 Outbox。
- `PatchApplier`：Git 工作区实现校验快照 commit 和解析器内容哈希；可扩展为 Provider 分支或隔离沙箱。

## 可观测性

阶段记录 `assistant_propose`、`assistant_apply`，标签包含 tenant、repository、snapshot 和 trace；指标覆盖提案数、变更文件数、token、幂等命中、拒绝和成功应用。日志与事件不保存源码正文或 diff。
