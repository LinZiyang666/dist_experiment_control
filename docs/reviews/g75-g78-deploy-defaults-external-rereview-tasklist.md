# g75-g78 部署默认值二次外审 tasklist

> 2026-08-11。对象是开发者针对首轮外审 F1/F8/F9/F10/F11 的全部 unstaged 回复与修补；不以回复中的“全绿”声明代替复测。
> 工作流按用户要求分界：先审查并暂存开发者基线与外审反例；之后外审者可直接修代码，但修补与最终二审报告留在暂存区外供对比。

## R. 开发者修补复审

- [x] R1. 固化 staged/unstaged 边界，逐项映射 F1/F8/F9/F10/F11 的实现、测试、文档与回复证据。
- [x] R2. 独立审查 NATS URL、listen host、loopback、端口 0 的纯校验与真实 nats.go/net listener 语义，补差分反例。
- [x] R3. 审查 F10 的 New/Run 两阶段测试是否无 TOCTOU、能稳定命中 policy 与 EADDRINUSE 两类错误。
- [x] R4. 复核 F8 runbook 命令、F9 结构预算、F11 lint 修复，以及首轮已关闭 F2–F7 是否回归。
- [x] R5. 运行 targeted、race、lint、make test/gates、99-unit e2e、simcluster hermetic 与受影响 live drill；区分 sandbox/setup 与产品红。
- [x] R6. 完成开发者基线审查后执行 `git add -A`，确认该时点工作区无 unstaged 内容。

## M. 外审授权修补（暂存边界之后）

- [x] M1. 对复审发现的代码/测试/文档问题直接修复，不改动暂存基线；补足非空和正反向守卫。
- [x] M2. 重跑所有受影响门禁，形成独立最终二审报告；报告首行 Pass/Fail，记录疑惑、问题、建议及 staged/unstaged 边界。
- [x] M3. 确认外审修补和最终报告全部留在暂存区外，便于 `git diff` 对比；不再次 `git add`。
