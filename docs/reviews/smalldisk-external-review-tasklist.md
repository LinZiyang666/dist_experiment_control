# smalldisk 独立外部审查 Tasklist

日期：2026-08-06

边界：`HEAD=71e4943`；审查开始时 index 为空，候选为 9 个 tracked unstaged 文件与
`docs/reviews/smalldisk-plan.md`、`internal/broker/xfer_converge_test.go` 两个 untracked 文件。
内部 plan/review 的结论只作线索，所有结论必须由实现、上游 JetStream 语义、独立反例或测试重新建立。

## 范围与契约

- [x] 通读 smalldisk plan、requirements、当前分布式架构、deploy gotchas、usage 的传输/容量契约，
  并核对历史 transfer 外审中已建立的安全、重试和原子提交不变量。
- [x] 对 11 个候选文件逐行审查；区分 sizing、existing-bucket convergence、provision retry、
  CLI/help、JS 常量和测试/golden 五个变化面。
- [x] 机械检查 wire/subject/error-code/CLI surface 是否漂移，确认 N-1 兼容和 ProtoVersion 不受影响。

## 容量数学与数值安全

- [x] 重建 nats-server 账户级与服务器级 reservation 公式，验证 `MaxStore`、`Store`、
  `ReservedStore`、replicas、events、per-session history、existing xfer bucket 的量纲与扣减口径。
- [x] 覆盖 N=1/N=3、0/1/多 session、现存/不存在 bucket、额外 operator stream、磁盘漂移、
  Store>Reserved、Reserved>Store、unlimited/unknown/zero/negative ceiling 等分支。
- [x] 检查所有 int/int64/uint64 转换、加减乘、headroom/margin/floor 比较是否有溢出、下溢、
  `MaxBytes<=0`（JetStream unlimited）或 off-by-one 导致半途写满。
- [x] 验证 2 GiB 最大对象的 chunk/message 开销余量、准入上限与实际 Object Store MaxBytes 一致。

## 现存 bucket 收敛与所有权

- [x] 证明健康 bucket 永不改 MaxBytes、永不自动扩容、只在已不可满足时缩容，且日志完整披露
  before/after/used/reserved/ceiling/原因。
- [x] 验证收敛不会缩到已用字节或在途上传以下，不会改变 storage/TTL/retention/compression/
  description 等 operator-owned 配置，也不会误降 replicas。
- [x] 审计并发 prepare、并发 create/update、bucket 在探测后变化/删除、leader/JS 切换、
  UpdateObjectStore 部分失败与重复执行的幂等性。
- [x] 验证 active-transfer 判定覆盖 push/pull、prepare/commit/timeout/watchdog 全生命周期，
  不存在 check-then-act 窗口导致在途对象被腰斩。

## Provision、超时与错误语义

- [x] 逐条证明 sizing budget 与 create/update attempt budget 独立，任何 JS 卡顿不会吃光后续预算，
  context/cancel/timer/goroutine 均能回收。
- [x] 验证永久 `errXferStoreTooSmall` 与瞬态 JS/meta/leader/storage 错误分类准确；永久条件零重试，
  瞬态仍走有界重试且不会 hot-loop、假成功或吞掉根因。
- [x] 检查 existing bucket 的 fast path、replica repair 与 resize 顺序，确保小盘修复不会破坏 HA
  replica convergence 或把可用桶误报为 create failure。
- [x] 审核拒绝/日志/CLI 文案是否与真实计算一致，是否泄漏内部细节或给出错误运维建议。

## JS 常量、查询与数据库

- [x] 验证 events/history 常量成为单一真相源后所有消费者、测试与文档一致；检查导出是否扩大了
  不必要 API，是否仍有裸字面量或手抄 reserve 腐化点。
- [x] 审核 session 计数 SQL 的状态范围、事务/只读句柄、错误路径、并发删除/创建偏差以及大数量级性能。
- [x] 检查 AccountInfo/StreamInfo/ObjectStore API 的真实 nats.go/nats-server 行为，而非只相信 fake。

## 测试诚实性与独立反例

- [x] 逐个审查新增/修改测试的 oracle、fixture、mutation strength、生产常量耦合、时间假设和 vacuity；
  特别检查 fake 是否复制实现公式从而同错同绿。
- [x] 为确认缺陷补职责命名的独立回归测试；对关键公式做边界/性质表，必要时用真实 embedded NATS
  重现 create/update、10047、MaxBytes 与 active-object 行为。
- [x] 运行定向测试、多次重复、`-race` 与传输 leak/concurrency 门；分类任何失败，不以重跑掩盖。

## 全局验证与交付

- [x] 运行 gofmt、diff hygiene、affected packages、`make gates`、`make test`、唯一全矩阵
  `make e2e-parallel`、lint/vet/build-tag/Darwin build；不运行无关 simcluster drill。
- [x] 审查完成后把开发者候选、tasklist 与独立测试全部加入暂存，记录 cached diff 指纹并确认工作树为空。
- [x] 获得冻结基线后才修改实现；审查者修复与最终报告全部留在暂存区外，并再次跑与风险相称的硬闸。
- [x] 形成 `docs/reviews/smalldisk-external-review.md`，首行为 Pass/Fail，列出 findings、疑惑、建议、
  精确测试结果、未覆盖面与暂存边界。
