# Coding Assistant 用例

`Service` 实现 MVP 的提案与审批应用流程：

1. 校验 Scope、权限、输入长度和幂等键，读取明确指定的成功快照。
2. 通过 `Retriever` 获取同快照证据，并过滤跨提交或无效引用。
3. 通过可替换 `ProposalGenerator` 生成摘要、解释、测试计划、风险及 diff；严格校验引用索引、文件数量、diff 大小和格式。
4. 原子保存编码会话与 `AWAITING_APPROVAL` 提案。
5. `Apply` 使用乐观锁认领提案；拒绝不会触发写操作，批准后由 `PatchApplier` 校验基础哈希并原子应用。
6. 成功后发布 `proposal.applied.v1`，重复 Apply 不重复写入，只重发已保存事件以支持恢复。

生产环境可替换生成模型、持久化和补丁目标；应用层不暴露供应商类型。默认 Git 适配器只修改工作区，不暂存、不提交。
