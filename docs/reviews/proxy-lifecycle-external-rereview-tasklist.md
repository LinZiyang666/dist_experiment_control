# proxy-lifecycle 独立外部复审 tasklist

> 基线：上轮外审 Fail 后的已暂存树。对象：开发者对 F1–F7 的回复及其全部未暂存修复。回复只作待证伪主张；结论按用户本轮口径裁定：存在重大问题则 Fail，无重大问题可 Pass，同时如实登记 Minor/建议。

## A. 范围与逐条回复核验

- [x] A1. 对账 index 与 developer delta，确认备份删除、ignore、文档/测试迁移及所有生产路径，没有把上轮外审测试静默放宽。
- [x] A2. 逐条核验 F1：server-owned context 的所有权、Start/Stop 并发、DNS/dial 取消、shutdown 顺序和 teardown 上界。
- [x] A3. 逐条核验 F2：accepted/upstream 同 key 绑定、dial/revoke/shutdown 竞态、解绑顺序和 map/fd/goroutine 终态。
- [x] A4. 逐条核验 F3：只对 persisted local `EADDRINUSE` 回退、错误解包、重试 single-use 安全、footprint/public port 更新。
- [x] A5. 逐条核验 F4：heartbeat 接线、(0,0)/unready 报告、single `repairProxy` 和 cluster reconcile 两模式恢复、公网端口与 OFF/exit 栅栏。
- [x] A6. 逐条核验 F5–F7：架构门递归/反空转/语义准确性、测试 API 表面、gotcha 口径、`.gitignore` 实际匹配。

## B. 对抗测试与门禁

- [x] B1. 先重跑上轮三项红测试，确认修复使其转绿而非 fixture 被弱化；运行新增 F4 行为/接线测试。
- [x] B2. 添加必要的独立复审测试或变异，优先覆盖 real heartbeat→single broker push、cluster 同端口恢复、Stop/revoke 竞态。
- [x] B3. 运行 affected packages、重复压力及 `-race`，检查新 context、全局 resolver、连接集合和 heartbeat 锁序。
- [x] B4. 运行 `make gates`、`make test`、`make e2e-parallel`、build、lint/vet/diff-check；区分 setup 与产品 verdict。
- [x] B5. 若 deploy 路径受影响，重建当前 simcluster 镜像并重跑 drill 73，核验 #33 STRANDED 臂、日志 oracle 与清理。

## C. 结论与交付

- [x] C1. 更新外审报告首行与复审章节，逐项给出证据、疑惑、Minor/建议及 NOT-COVERED。
- [x] C2. 全部 tasklist 打勾，最终 `git diff --check`，执行 `git add -A` 并核验无未暂存内容。
