# 测试体系革新增量外部复审 tasklist

> 2026-09-01。index 是首轮外审结束时的受审树；`git diff` 中 15 个既有文件是开发者对 F1–F5
> 及五条建议的回复。复审前不修改开发者实现。复审完成后只暂存这 15 个开发者文件；本 tasklist、
> 复审报告及随后由外审者直接完成的代码修补始终留在 staged 区外。

## A. 边界与逐条关闭

- [x] A1. 固化 staged/unstaged 路径集合与开发者回复，确认产品实现零新增、没有越过首轮审查边界。
- [x] A2. F1：验证 release 状态函数覆盖、默认 success 语义、前置 job 与 release 自身的失败传播及正负控制。
- [x] A3. F2：验证可信 polling helper 精确集合、loop body 行动判定、闭包/IIFE/方法别名与真实 d7 wait loop。
- [x] A4. F3：验证 loop/range/select 的 barrier/polling 判定，覆盖 timer-only、多 case、channel receive、空循环和嵌套闭包。
- [x] A5. F4：验证 os/exec import 别名、CommandContext、构造器函数值的声明/赋值/传参/return/复合字面量逃逸；证明无法解析必 whole，且唯一入口门同语义。
- [x] A6. F5 与建议 1–5：核对动态数字、WithLeader 边界、`-v` identity、closure stdout hash、B5 cache 因果与 port name/eol exact compare。

## B. 独立反例与回归

- [x] B1. 先原样复跑首轮四组反例，确认开发者没有删/弱化外审 oracle 且均转绿。
- [x] B2. 对 F1–F4 各补一轮外审自造变异；任何新假阴性先写最小 regression，再判定严重度。
- [x] B3. 检查新增扫描器是否产生假阳性、panic、无限递归、map/AST 顺序不稳定或跨文件漏判。
- [x] B4. 审查修改后的 inventory/dedupe/property tests 是否真的验证声明字段与错误路径，而非只改注释或期望值。

## C. 验证与交付边界

- [x] C1. 运行 targeted 普通/race/shuffle，`make gates`、`make test`、`make e2e-parallel`、lint、gofmt 与 diff checks；环境假红单独复跑归因。
- [x] C2. 判断 live simcluster 是否对本轮测试/runner 修复提供额外证据；若无，写明不运行原因。
- [x] C3. 写首行 Pass/Fail 的外部复审报告，逐条关闭首轮 finding，并记录新 finding、疑惑、建议和放行条件。
- [x] C4. 只把开发者 15 个修复文件加入 staged；确认 index 包含开发者回复、外审 tasklist/report/代码仍 unstaged。
- [x] C5. 按用户授权直接修复所有复审问题，保留外审改动 unstaged；补测试并循环验证，直到达到可放行状态。
- [x] C6. 完成最终 Pass 报告与边界核验：cached diff 无外审后续修补，unstaged diff 只含外审 tasklist/report/测试/修复。
