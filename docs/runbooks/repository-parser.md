# Repository Parser 运行手册

排查故障时，先按 `trace_id` 定位，再检查 `snapshot_id`。`INVALID_INPUT` 和
`REF_NOT_FOUND` 需要调用方修正输入。`GIT_FAILURE`、`PARSE_FAILURE` 和
`PERSISTENCE_FAILURE` 可以使用原幂等键重试。诊断故障时禁止记录仓库凭据或源码内容。
