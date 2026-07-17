# S6-S8 外部重审 Tasklist（round-3）

> 基线：round-2 已暂存内容；对象：开发者随后产生的未暂存修改。开发者回复只作待证陈述。

## A. 边界与承诺映射

- [x] A1. 固化 staged / unstaged / untracked 边界并确认本轮真实修改范围。
- [x] A2. 将 round-2 回复中的 B1/B2/M1/m1、枚举契约及 round-1 闭环承诺映射到代码文件和调用点。
- [x] A3. 区分已实现、部分实现、仅计划实现，避免以接受 finding 或计划替代证据。

## B. 断言库与 runner 静态审查

- [x] B1. 审查 `_AS_PRODUCT_RED` 初始化、归零、组合优先级、退出码和既有 `assert_bug` 调用影响。
- [x] B2. 审查所有 assert API 的描述、签名、gotcha、command 最小参数及空字符串校验。
- [x] B3. 审查结构化 verdict 的枚举一致性、字段边界、可解析性和日志兼容性。
- [x] B4. 审查 `run-drills.sh` 是否识别全部新 verdict、正确分栏计数并只把缺/非法 verdict 归为 infra。
- [x] B5. 审查九个目标 drill 对 `not_covered` / `assert_setup` 的迁移和裸 warning/setup 分支残留。
- [x] B6. 审查文档声称的四态、NOT-COVERED/INCOMPLETE、ASSERT-FAIL 与代码事实是否一致。

## C. 独立验证

- [x] C1. 运行 diff whitespace、sh/dash 语法检查及正常 GREEN 回归。
- [x] C2. 构造 PRODUCT-RED、SETUP-RED、ASSERT-FAIL、NOT-COVERED 及组合优先级状态矩阵。
- [x] C3. 构造所有 API 的缺参、空签名、空 command 对抗探针，检查是否 fail-closed 并到达 verdict。
- [x] C4. 用真实 runner 包装临时 PRODUCT-RED/NOT-COVERED drill，核对单 drill、suite 展示和退出状态。
- [x] C5. 评估远端 simcluster 复跑必要性；若业务路径已修改则执行相应活体测试与残留检查。

## D. 闭环与交付

- [x] D1. 逐项判定 round-2 B1/B2/M1/m1 的 CLOSED / PARTIAL / OPEN 状态。
- [x] D2. 复核 round-1 B1、M1-M6、m1 是否有任何真实代码或文档闭环。
- [x] D3. 形成首行 Fail/Pass 的 round-3 报告，记录疑惑、问题、建议和可复现证据。
- [x] D4. 检查报告/tasklist 一致性，将所有文件加入暂存并确认无 unstaged/untracked。
