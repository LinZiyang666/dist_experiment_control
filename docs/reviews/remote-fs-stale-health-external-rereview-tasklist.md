# remote-fs stale-health 独立外部复审 tasklist

> 基线：首轮外审已经暂存的 20 个文件。复审对象仅为返修后 6 个未暂存文件；开发者声明、11 次 drill 归因和三条绿色门禁均须独立重建。

## A. 差异与契约

- [x] A1. 粗读 staged/unstaged 状态、name-status、stat/numstat，确认返修 delta 为 6 个文件且无意外删除、生成物或新生产实现改动。
- [x] A2. 完整阅读 6 文件相对 index 的 diff，并与首轮 F1-F4、CLAUDE 闸门声明、gotcha 状态和 simcluster Mandate 对账。
- [x] A3. 核验返修没有越过本批 wire/config/生产行为边界；单列 #80 ledger 非本批裁决。

## B. F2/F3 架构门复核

- [x] B1. 审查 mint-site 路径传播：顺序块、双分支 if/else、缺失 else、提前 return、嵌套 if、loop/range、switch/type-switch、select、branch、defer/go/func literal 和包装器。
- [x] B2. 审查三值 lockset：Lock/Unlock、条件分支 join、缺失 else、defer、loop/switch/select、提前 return、嵌套 helper、接收者/字段身份及 unknown 的保守方向。
- [x] B3. 确认扫描范围只限 `internal/spawnsafe` 后仍覆盖 `Policy.mu` 的全部可达作废调用，同时不把同名跨包函数/无关 mutex 混入调用闭包。
- [x] B4. 保持首轮两项反例原义，添加必要的 reviewer 反例覆盖新算法盲区；做非空转/正负控制和针对性变异验证。

## C. F1 drill 与取证真实性

- [x] C1. 审查 Arm 1S-2/1S-3 oracle A/B 的命令执行次数、rc/output 文件传递、空值处理、shell 兼容、失败输出和断言之间状态保持。
- [x] C2. 审查 agent log cursor/count 是否真绑定本次请求，是否会把异步旧请求、后续 arm、重启追加日志或相同文本误算为“agent 收到本请求”。
- [x] C3. 审查 logs.sh 唯一映射/source、cleanup/trap、证据文件命名并发安全、旧证据污染、rc=124/125/126/127/信号等 timeout 语义。
- [x] C4. 从当前源码重建 sim 镜像并运行 drill 62；保留任一红灯，核对 oracle 是否真正区分 transport 终态与产品码，不用重跑绿色覆盖失败。
- [x] C5. 核验 #81 降级状态、转绿条件、11 次样本陈述和“不归因”措辞与可审计证据一致；识别报告回复中的重复、矛盾或过度结论。

## D. 验证与交付

- [x] D1. 运行 reviewer focused tests、architecture/determinism、affected race/repeat、simcluster hermetic tests、`make gates`、`make test`、`make e2e-parallel`、`make lint`、build、shell syntax 和 diff-check。
- [x] D2. 更新外审报告，新增独立复审 verdict、逐项裁决、findings/疑惑/NOT-COVERED 与完整命令结果；首行结论必须反映最新状态。
- [x] D3. 全部任务完成后 gofmt reviewer Go 测试、`git diff --check`，执行 `git add -A`，确认未暂存区为空且所有文件已暂存。
