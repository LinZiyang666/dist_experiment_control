# S6-S8 外部重审 Tasklist（round-2）

> 审查对象：相对上一轮已暂存基线的未暂存修改；审查者独立验证开发者回复，不继承其结论。

## A. 边界与回复核验

- [x] A1. 固化 staged / unstaged / untracked 边界，确认本轮开发者实际修改范围。
- [x] A2. 阅读开发者逐条回复，建立 B1、M1-M6、m1 的“声称修复 / 计划修复 / 未回应”映射。
- [x] A3. 核对文档回复与代码事实，识别把计划、机制或局部修改表述为问题闭环的情况。

## B. 共享断言库静态审查

- [x] B1. 审查新增计数器的初始化、跨 drill 清理、并发/子 shell 行为及退出码。
- [x] B2. 审查 `assert_setup`、`not_covered` 的输入、输出、失败优先级与机器可解析性。
- [x] B3. 审查 `drill_end` 在 assertion failure、setup failure、not-covered 组合下的判定矩阵。
- [x] B4. 审查 `run-drills.sh` 对第三种 verdict 的解析、汇总、退出状态和 `--no-retry` 行为。
- [x] B5. 搜索全部调用点，确认现存 warning/skip/环境缺失路径是否迁移到新机制。
- [x] B6. 核对 `PRODUCT-RED` 声称是否存在独立、可汇总的实现，而非仅靠日志文本。

## C. 独立验证

- [x] C1. 运行 shell 语法检查与最小正常路径回归。
- [x] C2. 构造 setup-fail、not-covered、assert-fail 与组合状态的对抗性探针，核对 verdict/rc。
- [x] C3. 用真实 runner 包装临时 drill，验证单 drill verdict 与 suite 汇总是否一致。
- [x] C4. 评估 simcluster 复跑必要性；若本轮实现触达远端路径则运行相应模拟并检查残留。

## D. 上一轮问题闭环

- [x] D1. 复核 B1：锁定验收格是否不再以 GREEN 表示未覆盖。
- [x] D2. 复核 M1-M6、m1：逐项标记已关闭、部分关闭、未关闭或新增回归。
- [x] D3. 复核文档 SSOT、验收表与审查结论是否同步。

## E. 交付

- [x] E1. 形成 round-2 外部审查报告，首行明确 Fail/Pass，并记录疑惑、问题、建议与测试证据。
- [x] E2. 完成全部 tasklist 项并检查报告引用、命令与结论的一致性。
- [x] E3. 将所有文件加入暂存，确认无 unstaged / untracked 内容。
