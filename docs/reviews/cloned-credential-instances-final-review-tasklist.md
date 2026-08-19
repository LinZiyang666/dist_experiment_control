# 克隆凭据实例最终外部复审 Tasklist

范围：在已暂存的上一轮基线之上，独立审查开发者本轮 7 个文件、`+177/-42` 的整改；不采信报告中的自报测试结果。审查结束后先暂存开发者版本，再实施外审修复，并让外审修复与最终报告保持未暂存。

- [x] A1. 对照 `CLAUDE.md`、requirements、plan、usage 与既有 review，确认放行契约和已知限制。
- [x] A2. 逐行审查本轮 7 文件 diff，核对 R1-R5 的实际闭环及回复准确性。
- [x] B1. 复跑 terminal reconnect refusal、cluster release、cross-family `ConfiguredNID` 三条独立守卫（含 race）。
- [x] B2. 覆盖 initial-register refusal、终止阈值、取消/重建时序及计数复位，检查是否仍可形成快速重连或幽灵订阅。
- [x] B3. 审查 cluster liveness DB 选择、未完成 wiring、并发/leader 语义与故障传播。
- [x] B4. 审查 `ConfiguredNID` 信任边界，包括直接租约、数字后缀真实设备、畸形输入和跨族预留。
- [x] B5. 复核 R3 shared-binary upgrade 与 R4 cluster process refile 的产品契约、测试 fidelity、文档披露和上线风险。
- [x] C1. 运行 focused packages、`make test`、`make gates`、drill lint；必要时运行 E2E/sim drill。
- [x] C2. 检查格式、race、结构预算、diff whitespace 与测试 oracle，识别新回归或代码异味。
- [x] D1. 给出开发者版本的严格 Pass/Fail，随后将开发者 7 文件完整加入暂存。
- [x] D2. 对确认的代码问题直接修复并增加回归测试；不得把修复加入暂存。
- [x] D3. 复验修复，形成以 `Pass` 或 `Fail` 开头的最终报告，并核对 staged/unstaged 两层差异。

执行结果：开发者版本因拒租终止循环、共享二进制升级、cluster process refile、连接收尾竞态及两处身份边界问题判定为 Fail，并已完整暂存。外审修复后 `make gates`、`make test`、99-unit `make e2e-parallel`、focused race 与真实 shared-home drill 84 全部通过；外审修复、测试、tasklist 和最终报告均保持未暂存。
