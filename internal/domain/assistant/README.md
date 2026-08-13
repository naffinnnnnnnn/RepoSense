# Coding Assistant 领域

本目录定义编码会话、变更提案、审批、校验结果及应用状态机。

- `CodingCommand` 固定租户、仓库与快照，要求 `repo:read`，并校验选中引用属于同一提交。
- `ChangeProposal` 保存逐文件基础内容哈希和标准 unified diff；解释型请求禁止携带文件修改。
- 提案从 `AWAITING_APPROVAL` 进入 `APPLYING` 后，只能到 `APPLIED` 或 `FAILED`；拒绝进入 `REJECTED`。终态不可逆。
- `Approval` 要求 `repo:write`。领域对象不依赖 LLM、Eino、Git 或数据库实现。
