# 设备使用清单（openpi pi0.5 实验拓扑）

> 本文件记录当前 openpi pi0.5 实验所使用的设备：硬件配置、角色、
> openpi 项目路径、公网暴露策略，以及 tmux 会话命名规则。
> tether 操作细节请参考 `docs/usage.md`（使用者）、`docs/broker-ops.md`（broker 运维）、`docs/cluster.md`（集群 HA）。
>
> 更新记录（新→旧）：
> - 2026-10-18 §2.9 新增 **h100-temp**（借用机，公网 **149.165.172.126**，1×H100 80GB HBM3，用法同 h100；⛔ 别人可能随时杀我们的进程：被杀后**不自动重跑**，推通知给 owner 处理）。
> - 2026-10-17 §2.8 新到一台 **A100**（公网 **149.165.152.232**），owner：**暂不能加入实验设备**；§2.6 **timan108 已卡死、停用**（10-17 05:43 起离线，owner：回不来，不用管）。
> - 2026-09-11 §1 **最适拓扑**：h100↔timan108、weilandserver↔timan107，一 server 一车队；worker 数按渲染显存定；env 构建锁。
> - 2026-09-11 §2.5 weilandserver 驱动是 `-server` 计算版，补装 `libnvidia-gl-595-server` 才用上 4090 渲染。
> - 2026-09-10 §2.7 新增 **h100**（公网 149.165.153.233，1×H100 80G，防火墙全开，1 TB 卷挂 `/data`）；旧 a100 离线，§2.4 存档。
> - 2026-09-10 §1 硬规则：**timan 族只能当 worker**。
> - 2026-08-20 §2.5.1 weilandserver 直连入口 `ziyanglin.com:23100-23199`，优先级见 §4.0。

---

## 1. 拓扑总览

```
timan108 (worker) ──公网 117 MB/s──► h100          149.165.153.233:232xx  1×H100 80G
timan107 (worker) ──公网──────────► weilandserver  ziyanglin.com:231xx    1×4090 49G
```

- **server**：加载 teacher 权重（+ cache 库）对外提供推理，被 worker 调用。
- **worker / client**：跑 RoboCasa365 或 LIBERO 模拟环境，发 policy 请求。
- 离线/存档节点见 §2.2-§2.4、§2.6。

### 最适拓扑（owner 裁定 2026-09-11）

**一 server 一车队，两条 lane 互不共用：**

| lane | server | worker | 入口 | 链路 |
|---|---|---|---|---|
| A | **h100** | **timan108** | `149.165.153.233:232xx` | 117 MB/s |
| B | **weilandserver** | **timan107** | `ziyanglin.com:231xx` | 历史主跑路径 |

**按实验线取参数**（配对与结构不变，只有这两列变）：

| 实验线 | server replica | worker / 每台 timan |
|---|---|---|
| RoboCasa365 × pi0.5 | **4** | **30** |
| RoboCasa365 × GR00T | **5** | 30 |
| LIBERO | 同上（按 teacher） | **64** |

⚠ LIBERO 的 64 不能照搬到 RoboCasa：厨房场景每个渲染上下文 1.5-1.6 GiB，64 个要 100 GiB，
timan107（64 GiB）和 timan108（72 GiB）都装不下。

**为什么不能两个车队挤一台 server**：实测 worker 10→20→30，吞吐 5.60→8.50→10.50 集/分，
每 worker 效率 0.560→0.425→0.350；同时两台 worker 机 load 仅 8.5/88 与 8.8/48，
server 也没有硬资源打满（功耗 413/700 W、显存 77%、CPU 60%）。排队在服务端单卡上，加 worker 只是排更长的队。

**weilandserver 当 server 不当 worker**：88 核 + 4090 49 GiB，拿它跑 30 worker 时自身 load 只有 8.5/88。

#### worker 数按渲染显存定（每 worker 1.5-1.6 GiB，上限 ≤75%）

| 机器 | 渲染显存 | 建议 | 占比 |
|---|---|---|---|
| timan107 | 8 × 8 = 64 GiB | 30 | 73%（实跑 4,550 集零故障） |
| timan108 | 3 × 24 = 72 GiB | 30 | 66% |

⚠ timan108 曾在 45 worker（98.7% 显存）把 NVIDIA 内核模块压死、整机失联（2026-09-10）。
致命的不是稳态占比，**是换任务时几十个 EGL 上下文同时创建/销毁的踩踏**——同日 weilandserver
30 个上下文挤一张 4090、98.3% 显存却没事，差别只在装了下面这把锁。

#### ⚠ env 构建锁（`episode_runner.py`，2026-09-11 起）

`max_cached_envs=1` 使每个 worker 在换任务时同时销毁+创建一个 EGL 上下文；N 个 worker 就是 N 对
并发操作砸向同一张卡，驱动按设备加锁，进程卡进 D 状态且 `SIGKILL` 无效。

修法：`_ensure_env` 的「销毁+创建」整段加跨进程文件锁 `RC365_ENV_BUILD_LOCK`
（默认 `/tmp/rc365_env_build.lock`，置空可关）。只改时序，不改 episode 行为，结果仍可比。

⚠ **`MjRenderContextOffscreen ... has no attribute 'con'` 的计数是最早的告警信号**，
比 journal 的 `error` 行早得多——一出现就降载。

#### server 内部

单卡多进程分时轮转会互相排队（h100 5 replica 时 util 100% 但功耗仅 59%、显存带宽 20%，launch-bound）。
待验方向：开 MPS，或减 replica + 加大 `BATCHING_MAX_BATCH_SIZE`。

### ⛔ 角色硬规则：timan 族只能当 worker（owner 裁定 2026-09-10）

**`timan1` / `timan107` / `timan108` 一律只作 client / worker（跑模拟环境、发 policy 请求），
永远不在上面起 policy server。** 不管它们的卡看起来多空、显存多大（timan1 有 4×A6000 48G、
timan108 有 3×A5000 24G），都不改变这条：**推理服务只落在 weilandserver / h100 这类 server 节点上。**

这条是硬规则，不是建议。规划算力时不要再把 timan 族算进 server 池，也不要因为"它闲着"就往上面放 server。

设备角色分工与 tmux 会话命名见下表 §2、§3。

---

## 2. 设备清单

### 2.1 timan107 — 模拟环境 client

| 项 | 值 |
|---|---|
| **角色** | client / worker（运行 openpi 模拟环境，发起 policy 请求）。⛔ **只能当 worker**，见 §1 硬规则 |
| **公网 IP** | 无（NAT 后，需通过 tether `expose` 暴露任何对外服务） |
| **CPU** | Intel Xeon E5-2650 v4 @ 2.20 GHz，2 socket × 12 core × 2 thread = **48 logical CPU** |
| **内存** | **220 GiB** |
| **GPU** | **8 × NVIDIA GeForce GTX 1080（每张 8 GiB，驱动 535.183.01）** |
| **openpi 目录** | `/scratch/zixuans8/openpi` |
| **uv 二进制** | `/shared/nas/data/m1/zixuans8/miniconda3/bin/uv` |
| **LIBERO 环境** | `/scratch/zixuans8/libero_sim`（conda prefix，python 3.8，含 libero + openpi_client；client/worker 用 `conda run -p /scratch/zixuans8/libero_sim python ...`） |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, …（统一所有设备命名，见 §3） |

GPU 备注：8 张 GTX 1080 单卡仅 8 GiB 显存，**不适合**跑 pi0.5 推理（权重装不下），
所以这台只承担 client / 模拟环境角色；推理调用走 a100 或 jupyter-ziyang10。

注意：
- 用户主目录可能不是 `zixuans8`，路径是 `/scratch/zixuans8/openpi`，使用前
  确认是否有读写权限或共享挂载语义。
- client 通常不需要对外暴露端口；如果要把客户端 UI / debug HTTP 透出来
  用 `tether expose timan107 --local <port> --name <name>`，由 pc732（weiland.top）broker 分配 14000-14999 公网端口。

### 2.2 jupyter-ziyang10 — pi0.5 server（资源受限）

| 项 | 值 |
|---|---|
| **角色** | server（运行 pi0.5 模型推理） |
| **公网 IP** | 无（NAT 后） |
| **CPU 配额** | **最多 10 cores**（cgroup `cpu.max=1000000 100000` 强约束；宿主物理可见 60 vCPU AMD EPYC-Milan） |
| **内存配额** | **最多 32 GiB**（cgroup `memory.max=34359738368`；宿主物理可见 235 GiB） |
| **GPU** | **1 × NVIDIA H200 NVL（143771 MiB ≈ 140 GiB HBM3e，驱动 570.211.01）** |
| **openpi 目录** | `/home/ziyang10/openpi` |
| **模型权重** | `/home/ziyang10/.cache/openpi/openpi-assets/checkpoints/`（含 `pi05_libero_pytorch`、`pi05_base_pytorch`） |
| **uv 二进制** | `/home/ziyang10/.local/bin/uv` |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, … |

**重要约束**：
- 这是 JupyterHub 容器，CPU/内存上限由平台 cgroup 强制，**超用会被 OOM
  kill 或 throttle**。pi0.5 推理 batch / worker 数要按 10C/32G 调，不要照搬
  a100 上的配置。
- 反差点：GPU 是 H200 NVL（140 GiB HBM3e），单卡显存比 a100 那张 A100-40GB
  还大约 3.5×；瓶颈在 CPU/内存而非 GPU，dataloader / 预处理线程数要按
  10 core 调小。
- 容器生命周期不一定与物理机一致，重启后 `/home/ziyang10/openpi` 持久，
  但临时进程 / tmux 可能丢失，重要任务用 `tmux new -s srvN -d` + 输出落盘。
- **`tether exec` 进来的 shell `HOME=/home/ziyang10/.tether-agent`，不是 `/home/ziyang10`**。
  任何用 `~/.cache` / `Path.home()` 的程序（openpi 找 tokenizer / checkpoint 等）必须
  先 `export HOME=/home/ziyang10`，否则会去 `.tether-agent/.cache/...` 找文件 fail。

### 2.3 jupyter-xuanlel2 — pi0.5 server（资源受限，与 ziyang10 同 JupyterHub 集群）

| 项 | 值 |
|---|---|
| **角色** | server（运行 pi0.5 模型推理） |
| **公网 IP** | 无（NAT 后） |
| **CPU 配额** | **最多 10 cores**（cgroup `cpu.max=1000000 100000` 强约束；宿主物理可见 60 vCPU AMD EPYC-Milan） |
| **内存配额** | **最多 32 GiB**（cgroup `memory.max=34359738368`；宿主物理可见 235 GiB） |
| **GPU** | **1 × NVIDIA H200 NVL（143771 MiB ≈ 140 GiB HBM3e，驱动 570.211.01）** ⚠ 与 ziyang10 同型号，但**不同物理 GPU**（UUID 不同），跨机对比要注明 |
| **openpi 目录** | `/home/xuanlel2/openpi` |
| **模型权重** | `/home/xuanlel2/.cache/openpi/openpi-assets/checkpoints/`（含 `pi05_libero_pytorch`、`pi0_aloha_sim`、`pi0_base`） |
| **uv 二进制** | `/home/xuanlel2/.local/bin/uv`（0.11.16） |
| **tmux 二进制** | `/home/xuanlel2/miniforge3/bin/tmux`（**镜像无系统 tmux**，必须用全路径或 `bash -lc` 进 conda env 让 PATH 改写） |
| **uid / gid** | `1273722 / 51000`（NFS uid 映射坑：home 上 mkdir 后 owner=nobody，warmup_dump_root 之类 owner 严格校验用 `/tmp/xl_<unique>_xxx` 而不是 home） |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, …（统一所有设备命名，见 §3） |

**重要约束**（与 ziyang10 同源 — 都是 JupyterHub 容器）：
- 与 ziyang10 完全一致的 cgroup（10 cores / 32 GiB 内存）；超用会被 OOM kill 或 throttle。
- 与 ziyang10 同 H200 NVL 型号，但**物理上是另一块 GPU**（跨机对比 SR / latency 时务必标注 "different physical GPU"）。
- 容器生命周期不一定与物理机一致；重要任务用 `tmux new -s srvN -d` + tee 落盘 + stdout 留在 pane 内可见。
- **`tether exec` 进来的 shell `HOME=/home/xuanlel2/.tether-agent`，不是 `/home/xuanlel2`**。任何用 `~/.cache` / `Path.home()` 的程序必须先 `export HOME=/home/xuanlel2`。
- **镜像没有 `fuser`**：端口释放 fallback 用 `pkill -9 -f "[s]erve_policy.py"` + `ss -tlnp \| grep :8000 || echo free`。
- **镜像没有系统 `tmux`**：用 `/home/xuanlel2/miniforge3/bin/tmux` 全路径，或者 `bash -lc` 让 `.bashrc` 把 PATH 加进 conda env 再调 `tmux`。

### 2.4 a100 — pi0.5 server（公网入口）【存档，已被 §2.7 h100 取代】

> **⚠ 状态（2026-09-10 复核）**：`tether node ls` 中仍然**没有 a100**。同一台 Jetstream2 实例名
> （hostname `vla-cache`）现在以 **h100** 的身份在线，公网 IP 变成 149.165.153.233、用户变成
> `exouser`、卡换成 H100 80G —— 见 **§2.7**。本节只作历史存档，不要照着它的 IP / 路径下命令。

> **⚠ 2026-05-26 换机**：原 a100（公网 149.165.151.106）已退役（数据/代码/模型擦除 + tether agent 卸载 + broker evict）。当前 a100 是新机（hostname `vla-cache`，公网 **149.165.152.105**），全套从旧机 rsync + uv 重建环境后接管 nid=`a100`。下表为新机。

| 项 | 值 |
|---|---|
| **角色** | server（pi0.5 推理节点） |
| **公网 IP** | **149.165.152.105**（hostname `vla-cache`；唯一有公网 IP 的实验机） |
| **GPU** | **1 × NVIDIA A100-SXM4-40GB（40960 MiB）** |
| **磁盘** | 根分区 58 GiB（注意：比旧机小，cache_artifacts 9.5G + checkpoint 6.8G 后约 22G 余量） |
| **openpi 目录** | `/root/openpi`（git remote 私有仓库，github_deploy_key 已配，可 git pull） |
| **模型权重** | `/root/.cache/openpi/openpi-assets/checkpoints/pi05_libero_pytorch` |
| **uv 二进制** | `/usr/local/bin/uv`（0.11.7）。⚠ `uv sync` 后须手动打 transformers 补丁：`cp -r src/openpi/models_pytorch/transformers_replace/* .venv/lib/python3.11/site-packages/transformers/`（否则 serve_policy 报 transformers_replace not installed） |
| **tether agent** | SYSTEM systemd 服务 `tether-recover.service`（`ExecStart=/root/.local/bin/tether agent --session lab --nid a100`，Environment=HOME=/root，Restart=always）。⚠ 经 `sudo` / 系统服务运行 agent 时务必 `HOME=/root`，否则找不到 `/root/.tether` 配置 |
| **SSH** | `ubuntu@149.165.152.105`（用 Windows 侧 `id_rsa`，非 root；ubuntu 有免密 sudo） |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, … |

公网角色：
- a100（149.165.152.105）是 3 台实验机中唯一有公网 IP 的；
- **broker 不在 a100**，而在 **pc732**（域名 `weiland.top` → 155.98.36.32，tether 节点 `pc732`）；`tether admin` 须在 pc732 本机以 root/tether 用户跑（pc732 上 `lzy666` 有免密 sudo）；
- 前两台（timan107 / jupyter）无公网 IP，对外服务通过 `tether expose` 打到 broker 公开的 `14000-14999` 端口段（如 jupyter 推理 → `weiland.top:14000`）。

### 2.5 weilandserver — pi0.5 server + LIBERO client（自给自足节点）

> ⚠ **驱动是 `-server` 计算版**（`nvidia-dkms-595-server` 595.71.05），**故意不带图形栈**，
> 所以原本只有 mesa 的 EGL ICD，MuJoCo 走 llvmpipe 软件渲染（7.45 ms/帧，且像素与其它机器不一致）。
> 2026-09-11 装了版本严格匹配的 `libnvidia-gl-595-server`（不动内核模块），现在
> `GL_RENDERER = NVIDIA GeForce RTX 4090`、1.54 ms/帧。**新机装完驱动务必验一次 `GL_RENDERER`。**

> 采集日期 2026-08-13。这台是后加入的自有物理机，既跑推理 server，也在本机跑 LIBERO 模拟 client，
> 单机即可闭环（client 走 `127.0.0.1`，不经 broker）。

| 项 | 值 |
|---|---|
| **角色** | server + client（两个角色都能担；本机闭环时 client → `127.0.0.1:<port>`） |
| **公网 IP** | **有（2026-08-20 起）**：`ziyanglin.com` → `140.177.159.24`，交换机 NAT 转发 **TCP 23100-23199**，**1:1 映射**（公网 `:23150` → 本机 `:23150`）。LAN `192.168.0.200` / `192.168.20.200`。该段之外仍走 `tether expose`（现役 `wls-ssh`→`:22`）。详见 §2.5.1 |
| **OS** | Ubuntu 24.04.4 LTS |
| **CPU** | 2 × Intel Xeon E5-2696 v4 @ 2.20 GHz，2 socket × 22 core × 2 thread = **88 logical CPU** |
| **内存** | **251 GiB** |
| **GPU** | **1 × NVIDIA GeForce RTX 4090（49140 MiB ≈ 48 GiB，大显存改装版）**，driver **595.71.05** |
| **磁盘** | 913 GiB 根分区（2026-08-13 已用 284G / 可用 582G） |
| **openpi 目录** | `/home/weiland/projects/openpi`（uv venv `.venv`，python 3.11） |
| **uv 二进制** | `/home/weiland/.local/bin/uv` |
| **模型权重** | `/home/weiland/.cache/openpi/openpi-assets/checkpoints/pi05_libero_pytorch` |
| **lerobot 环境** | venv `/home/weiland/lerobot_venv`（+ conda env `lerobot`），用于 SmolVLA / ACT sidecar |
| **LIBERO 环境** | **`/home/weiland/libero_sim`**（conda prefix，python 3.8.20；2026-08-13 按 timan107 配方复刻，见下方“LIBERO 环境”小节） |
| **conda** | `/home/weiland/miniconda3/bin/conda`（26.5.3） |
| **tether agent** | 用户态进程 `tether agent --session lab --nid weilandserver`（非 systemd 服务） |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, …（现役还有 `sml*` / `sm*` / `acts*` 系列 sidecar 会话，来自 ablation 实验） |

**GPU（2026-08-26 更新：旧卡已退役拆除）**：本机现役 **一张 RTX 4090（49140 MiB）**，无冷卡缺陷，**不需要保温脚本 —— 开机后可直接起 GPU 负载**。

<details>
<summary>已退役的 48G 改装 4090 故障史（PCI 04:00，2026-08-26 由 owner 物理拆除）</summary>

该卡有确定性硬件缺陷，判为显存接口级时序余量退化（clamshell 24 颗双载信号完整性），软件因素全部洗清。

- **早期（2026-08-20）**：触发条件是冷态（≤36 °C）陡热爬坡 × 满带宽显存流量两条件叠加，单独任一不触发。三种死法：静默计算错误（4,069 万个，67–71 °C 非过热，零 Xid 零报警）、静默算死挂起（只有 `proc'd` 冻结可见，`errors: 0` 是僵尸读数）、`Xid 31 MMU Fault @0x0` → `175 GSP timeout` → `154 Reset Required` 三次打死推理 server。当时的缓解是保温脚本常驻（恒温带 54/62 °C，~432 MiB）；降功率锁频（`-pl 350` + `-lgc`）早已证伪。
- **末期（2026-08-26，退役当日）**：恶化到**通电即不稳定**。装入第二张卡后同日崩溃三次，最后一次的 Xid 报告者是 `nvidia-smi` 与保温脚本自身，gpu_burn 从未成功分配显存（GPU0 全程只有 441 MiB = 保温用量）⇒ 与负载、温度（63 °C）均无关，纯查询即可触发 GSP RPC 超时。owner 据此判定退役。
- 全部取证：`weiland-wsl:/mnt/c/Users/lzy66/Downloads/gtp_logs/`（送修报告 `REPAIR_REPORT.md`）+ 本机 `/home/weiland/gpu_fault_reports/`、`/home/weiland/gtp_logs/`。

**仍然通用的经验（与具体显卡无关）**：
- 判 gpu_burn 结果必须看三件套：`errors` + **`proc'd` 是否持续推进** + 终判行；错误分布用 `tr "\r" "\n" | grep -aoE "errors: [0-9]+" | sort | uniq -c` 看全程，`tail -1` 抽查会漏掉整段错误窗口。
- **wedge 后软重启不可靠**：GPU 卡死后 `systemctl reboot` 可能被 D 状态进程 + GSP RPC 串行超时钉死在半关机态。规程：软重启下发 5 分钟未回 ONLINE = 关机挂死，直接物理重启。

</details>

**GPU 备注**：4090 是**独占卡**（util 与 memory 都是真实信号，不像 jupyter 系列共享卡只能看 `memory.free`）。
显存 48 GiB 可同时驻多个 pi0.5 server + 若干小模型 sidecar；起新负载前照例先 `nvidia-smi` 查 `memory.free`。

**LIBERO 环境（2026-08-13 新建，与 timan107 同配方）**：

| 项 | timan107 | weilandserver |
|---|---|---|
| prefix | `/scratch/zixuans8/libero_sim` | `/home/weiland/libero_sim` |
| python | 3.8.20 | 3.8.20（一致） |
| 关键包 | libero 0.1.1 / robosuite 1.4.0 / mujoco 3.2.3 / bddl 1.0.1 / robomimic 0.2.0 / numpy 1.22.4 / torch 1.11.0+cu113 | 同左（按 timan107 `pip freeze` 逐版本 `--no-deps` 锁定，共 126 包） |
| `openpi-client` | editable → `/scratch/zixuans8/openpi/packages/openpi-client` | editable → `/home/weiland/projects/openpi/packages/openpi-client` |
| LIBERO 配置 | `~/.libero/config.yaml` 指向 prefix 内 assets/bddl_files/init_files | 同样式（路径换成本机 prefix） |

两处必须知道的差异：

1. **libero 包用的是 timan107 的补丁版，不是 PyPI 原版**。PyPI 的 libero 0.1.1 在
   `libero/libero/benchmark/__init__.py` 里调用 `torch.load(path, weights_only=False)`，
   而 `weights_only` 参数在 torch 1.11 不存在 → `TypeError`。timan107 上该行已被改成
   `torch.load(init_states_path, )`。复刻时直接从 timan107 打包整个 `libero/` 包覆盖过来，
   不要用 `pip install libero`。同理 `egl_probe` 需要 CMake + EGL 头文件才能从源码编译，
   本机没有编译依赖，直接搬 timan107 已编译好的 `egl_probe/` 目录。
2. **NVIDIA 驱动是 `-server`（compute-only）变体，缺 EGL 用户态库** —— 装的是
   `nvidia-headless-595-server` / `libnvidia-compute-595-server`，`/usr/share/glvnd/egl_vendor.d/`
   里只有 `50_mesa.json`，没有 `libEGL_nvidia.so` → MuJoCo/robosuite 离屏渲染直接
   `EGLError`。**免 root 的解法**（已落地）：
   ```bash
   # 1) 下载与内核模块版本精确匹配的 GL 包（apt download 不需要 root）
   mkdir -p ~/nvidia-gl/dl && cd ~/nvidia-gl/dl
   apt download libnvidia-gl-595-server          # 595.71.05-0ubuntu0.24.04.1
   dpkg -x libnvidia-gl-595-server_*.deb ~/nvidia-gl/root
   # 2) 由 conda activate 钩子自动注入（已写入 libero_sim/etc/conda/activate.d/nvidia_egl.sh）
   export __EGL_VENDOR_LIBRARY_DIRS=~/nvidia-gl/root/usr/share/glvnd/egl_vendor.d
   export LD_LIBRARY_PATH=~/nvidia-gl/root/usr/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH
   ```
   ⚠ 换驱动版本后必须重下匹配版本的 GL 包（用户态与内核态版本必须一致），
   `libnvidia-gl-595`（非 `-server`）候选是 595.84，**版本不匹配，不要装**。

**使用方法（与 timan107 完全一致的调用形态）**：

```bash
# 本机闭环：client → 本机 server（不经 broker）
tether exec weilandserver -- bash -lc '
  export HOME=/home/weiland
  cd /home/weiland/projects/openpi
  MUJOCO_EGL_DEVICE_ID=0 PYTHONPATH=. ~/miniconda3/bin/conda run -p /home/weiland/libero_sim \
    python examples/libero/main.py \
      --host 127.0.0.1 --port 8000 \
      --task-suite-name libero_spatial --task-ids 0 --num-trials-per-task 50 \
      --cuda-visible-devices 0 --num-workers 1 \
      --save-episode-results --episode-results-path <out>/results.json
'
```

- `conda run -p` 会触发 activate 钩子 → EGL 变量自动生效；**直接调 `libero_sim/bin/python` 不会生效**，
  那种调法必须自己 export 上面两个变量。
- `--num-workers` 上限同样是 15（MuJoCo 单 GPU EGL context 限制），要更高并发就开多进程。
- 验收状态：2026-08-13 已跑通 1-ep 端到端 smoke（libero_spatial task 0 → 本机 `:8000` pi0.5 server，
  `success=true`），GPU EGL 渲染 256×256 正常。

#### 2.5.1 直连公网入口 `ziyanglin.com:23100-23199`（2026-08-20 起，**优先于 tether expose**）

交换机把公网 **TCP 23100-23199** 段 NAT 到 weilandserver，**1:1 映射**：公网 `:23150` 打到本机 `:23150`。

**与 `tether expose` 的关键差别**：`expose` 能把**任意**本地端口映到 broker 分配的端口；直连段**不做端口转换**，
所以**服务必须自己监听在 23100-23199 之内**。把 server 起在 `:8000` 再指望公网 `:23100` 能连上，是连不通的。

**ufw 是必经的一道**（2026-08-20 实测踩过）：本机 ufw 默认 `deny (incoming)`，原白名单只有 22/8000/8080 且都限
`192.168.0.0/16`。交换机映射正确、路由通、服务在听，但**入站被 ufw 丢弃**，现象是纯超时、listener 侧零连接、
而本机 `127.0.0.1` 自连正常。已加常驻规则：

```bash
sudo ufw allow 23100:23199/tcp comment "public range via ziyanglin.com switch NAT"
# 校验：ufw status | grep 23100
```

**实测记录（2026-08-20）**：

| 探测 | 来源 | 结果 |
|---|---|---|
| `ziyanglin.com:23100` | timan107（真外网，入站源 IP `192.17.58.207`） | ✅ 通 |
| `ziyanglin.com:23199` | timan107 | ✅ 通（区间上界） |
| `ziyanglin.com:23200` | timan107 | ✅ 被拒（区间外，证明规则有边界） |
| `ziyanglin.com:23100` | 本机 WSL | ✅ 通（源 IP 显示为 `140.177.159.24`，同 NAT hairpin 回环） |

出口 IP 自证：weilandserver 上 `curl ifconfig.me` 返回 `140.177.159.24`，与 `ziyanglin.com` 解析一致 ⇒ 该公网 IP 确实落在本机。

⚠ **该段无任何鉴权**。与 `tether expose` 同理（见 §4 注意事项），但直连段暴露面更大——它不经 broker、
不受 session 成员资格约束，任何人扫到端口即可连。pi0.5 推理 server 本身**没有** token/TLS，
把它起在这个段上等于对公网开放推理算力。敏感服务要么自带鉴权，要么继续走 `tether expose`。

---

### 2.6 已入网但未配置 openpi 的节点

下列节点已在 tether session `lab` 内在线，但**没有 openpi repo / 环境**，用前需要先部署：

| 节点 | 硬件 | 备注 |
|---|---|---|
| `timan1` | 48 logical CPU / 503 GiB RAM / **4 × RTX A6000（各 48 GiB）** | 车队里最大的一块未开发算力。⛔ **只能当 worker**，卡再大也不起 server（§1 硬规则） |
| `timan108` | 48 logical CPU / 251 GiB RAM / **3 × RTX A5000（各 24 GiB）** | ⛔ **2026-10-17 起停用**：05:43 起 tether OFFLINE，owner 判定已卡死、回不来、不用管；不探测、不排期、不计入容量。以下为历史记录——未开发；2026-09-10 实测**完全空闲**（load 0.16、显存全 0、251 G 内存只用 5 G），`/srv/local` 是 11 T md0 RAID5、剩 9.9 T（根盘只剩 51 G，别往根盘写）。⛔ **只能当 worker**（§1 硬规则） |
| `weiland-optiplex-7050` | 小型机 | 跑 daemon（`daemon-console:14060` / `daemon-gw:14766`），非计算节点 |
| `racknerd` | 1 vCPU VPS | 网络中转，非计算节点 |
| `weiland-wsl` | 开发机（WSL2） | 本地 repo + 实验原始数据主副本，非计算节点 |

### 2.7 h100 — 单卡 H100 server（公网直连，2026-09-10 入网）

> **状态**：ONLINE，openpi + GR00T 双环境端到端验通，三份 RoboCasa 权重与 20 个 cache 库就位（均核过 sha256）。
> 这台是 Jetstream2 新实例，hostname 与旧 a100 同为 `vla-cache`，但**公网 IP 不同**（旧 a100 是
> 149.165.152.105，已离线；本机 149.165.153.233），用户从 `root` 变成 `exouser`。

| 项 | 值 |
|---|---|
| **角色** | server（推理节点，公网直连；装上 sim 后也可当自给自足节点） |
| **公网 IP** | **149.165.153.233**（hostname `vla-cache`；内网 10.0.125.146/24） |
| **OS / 内核** | Ubuntu 24.04.5 LTS / 7.0.0-31-generic |
| **GPU** | **1 × NVIDIA H100 80GB HBM3（81 559 MiB）**，驱动 595.84 / CUDA 13.2，SM 1980 MHz，功耗墙 700 W，persistence mode 开 |
| **实测算力** | 预热后 BF16 8192³ matmul 3.44 ms → **319 TFLOP/s**（冷启动只有 159，量之前务必先跑十次预热） |
| **CPU / 内存** | **20 logical CPU** / **230 GiB RAM** |
| **磁盘** | 根分区 58 GiB（环境 + 三份权重后只剩 11 G，**别再往根盘放大东西**）。**2026-09-10 挂了一块 1 TB 卷** `/dev/sdb`（ext4）→ `/media/volume/OmniData`，并做了软链 **`/data -> /media/volume/OmniData`**，所以 cache 库的绝对路径与 weilandserver 完全一致。`/software` 是 9.8 T 的 **只读** ceph 挂载，写不了 |
| **用户 / 权限** | `exouser`（uid 1001），**免密 sudo**，在 `docker` 组 |
| **HOME** | `/home/exouser` —— ⚠ `tether exec` 默认 HOME 是 `~/.tether-agent`，远端命令必须 `export HOME=/home/exouser` |
| **uv 二进制** | `/home/exouser/.local/bin/uv`（0.12.12）；托管的 python 3.11.16 在 `~/.local/share/uv/python/` |
| **openpi 目录** | `/home/exouser/openpi`，分支 `Ziyang`，`c0dac3a`。⚠ **仓库是公开的**（`https://github.com/LinZiyang666/openpi.git`），直接 `git clone` 即可，不需要 deploy key |
| **venv** | `/home/exouser/openpi/.venv`（python 3.11.16）；torch **2.7.1+cu126**（`cuda.is_available()=True`，认出 H100），jax 0.5.3 → `CudaDevice(id=0)` |
| **transformers 补丁** | ⚠ 与 a100 同：`uv sync` 后必须 `cp -r src/openpi/models_pytorch/transformers_replace/* .venv/lib/python3.11/site-packages/transformers/`，否则 `serve_policy` 报 transformers_replace not installed。**已打** |
| **模型权重** | `/home/exouser/ckpt/`：`pi05_robocasa_pytorch`（7.2 G）、`n15_robocasa_tp`（7.1 G，GR00T teacher）、`n15_robocasa`（7.1 G，非 tp 变体）。三份都与 weilandserver `/data/ckpt/` 同源、sha256 逐文件核对过 |
| **GR00T 侧** | 包 `/home/exouser/gr00t_n15`（NVIDIA/Isaac-GR00T，pin 在 `4af2b62`，与 weilandserver 同 commit）；venv `/home/exouser/gr00t_n15_venv/.venv`（python 3.11，torch **2.5.1+cu124**、flash-attn 2.7.4.post1、decord 0.6.0，全部认出 H100）。gr00t **不安装**，靠 `PYTHONPATH` 挂载，与 weilandserver 的跑法一致 |
| **tokenizer 缓存** | `/home/exouser/.cache/openpi/big_vision/paligemma_tokenizer.model`（4.2 M，从 weilandserver 拷来）—— ⚠ 必须预置，原因见下 |
| **tmux 会话前缀** | `srv0`, `srv1`, … （统一规约，见 §3） |

#### 防火墙 / 对外暴露（2026-09-10 实测）

**完全开放，不需要 `tether expose`，直连即可。**

| 层 | 状态 |
|---|---|
| ufw | service `enabled` + `active`，但 `ufw status` = **inactive**（即逻辑上关着，重启后仍是关着） |
| iptables INPUT | `-P INPUT ACCEPT`，**零条规则** |
| nftables | service disabled；`nft list ruleset` 的 112 条全部来自 docker 的 NAT/FORWARD 链，不拦 INPUT |
| OpenStack 安全组 | 不限制任意 TCP —— 实测从 WSL 侧连 **22 / 8000 / 9000 / 14000 / 23200 / 23299 全部 OPEN** |

⚠ **不要去 `ufw enable`**：本机没有别的兜底规则，一旦启用会同时切掉 SSH 与 tether agent 的出站重连路径，
把机器锁死；owner 的要求是放开，现状已经是放开。

端口段约定：为了和 weilandserver 的 `23100-23199` 区分开，**h100 用 `23200-23299`**，
client 侧直接写 `149.165.153.233:232xx`，不经 broker。

#### 磁盘：根盘 58 G 紧，1 TB 卷是主力

**2026-09-10 挂了 1 TB 卷**：`/dev/sdb` → `/media/volume/OmniData`（ext4），软链 **`/data -> /media/volume/OmniData`**，
于是 cache 库路径与 weilandserver **逐字相同**，yaml 里的 `preload` 不用改。
另有 `/scratch/zixuans8` 软链到同一卷，与 timan107 路径一致，sim venv 可原样搬来直接跑。

| | 容量 | 放什么 |
|---|---|---|
| 根盘 `/` | 58 G（**剩 ~11 G**） | 系统、openpi repo+venv、三份权重、gr00t venv。⚠ 别再往这里放 20 G 级的东西 |
| 卷 `/data` | 1007 G（剩 ~800 G） | **20 个 cache 库共 200 GiB**、sim 环境 33 G |

未拿的：`ckpt/n15_libero_*`（LIBERO 线，要用再拉约 5 min/份）、`ckpt/pi05_robocasa` JAX 版（本仓库已禁用 JAX 推理）。

传库用 `~/tmp_rit/copy_libs_to_h100.sh`：weilandserver 起 rsync daemon → h100 拉，**不经 tether**，
实测 31 MB/s（200 GiB 约 1.8 h）。走 timan107 的链路快得多（99 MB/s）。

#### 起服务（实测命令）

```bash
# pi0.5 teacher → 149.165.153.233:23200
tmux new -s srv0 -d "cd /home/exouser/openpi && uv run scripts/serve_policy.py \
  --replicas 4 --replica-spawn-batch 1 --port 23200 --cache-config <arm.yaml> \
  policy:checkpoint --policy.config pi05_robocasa --policy.dir /home/exouser/ckpt/pi05_robocasa_pytorch"

# GR00T teacher → :23201（--cache-config 不给就是纯 teacher）
CKPT=/home/exouser/ckpt/n15_robocasa_tp/gr00t_n1-5/foundation_model_learning/target_posttraining/atomic_seen/checkpoint-60000
tmux new -s srv1 -d "cd /home/exouser/openpi && \
  HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 NO_ALBUMENTATIONS_UPDATE=1 \
  PYTHONPATH=/home/exouser/gr00t_n15:/home/exouser/openpi/src:/home/exouser/openpi \
  /home/exouser/gr00t_n15_venv/.venv/bin/python exp/robocasa365/serve_groot_n15.py \
  --checkpoint \$CKPT --port 23201 --concurrent"
```

pi0.5 起到监听约 70 s/replica（带 cache 时更久，每 replica 要读 26 GiB 库）；GR00T 日志走到
`POLICY-READY` 即已跑过一次真实推理。实测公网推理 280/232 ms，动作 `(50,12)` 有限。
**两个 teacher 可同时挂**（一起仅 14.5 GiB / 81.6），不像 weilandserver 必须切换前杀干净。

⚠ 起 pi0.5 带 cache 时**内存是上限不是显存**：每 replica 约 30 GiB RSS（含 26 GiB 库），
4 个约 120 GiB / 230 GiB。启动脚本 `~/rit/start_pi05_h100.sh` 内置守门：可用内存低于 40 GiB 就中止。

#### 装机踩到的坑

1. **`policy:default` 走不通**：本仓库禁用了 JAX 推理（`JAX inference is temporarily disabled`），
   默认策略解析到的是 JAX 权重。必须 `policy:checkpoint` + PyTorch 权重。
2. **`serve_policy.py` 的顶层 flag 必须写在 `policy:*` 子命令之前**，写在后面 tyro 直接打 helptext 退出，
   看着像"没报错但没起来"。
3. **GCS 下载在当前依赖组合下是坏的**：`TypeError: int() argument ... not 'NoOpCallback'`
   （fsspec 2026.1.0 把 `callback=` 透传给 gcsfs 0.8.0 → aiohttp query params → yarl 拒绝序列化）。
   weilandserver 版本相同、只是早已缓存，**不是 h100 的问题，别查依赖**。
   后果是 `paligemma_tokenizer.model` 下不来、server 起不来；解法是从 weilandserver 拷那 4.2 M 文件。
4. **rsync daemon 会改写绝对软链**：`munge symlinks` 默认开，venv 的解释器链变成 `/rsyncd-munged/...`；
   关掉后又变成丢前导斜杠的相对链。两次都让 venv 起不来，最后在目标机直接 `ln -sfn` 补回绝对路径。
5. `tether push` 默认拒绝覆盖（`dst_exists`），重传加 `--force`。
6. 从别机 freeze 出来的依赖表里会有本机绝对路径（`-e file:///home/weiland/projects/openpi/packages/openpi-client`），
   直接拿去装报 `Distribution not found`，要先改写路径。

### 2.8 a100（新，2026-10-17 到位）—— ⛔ 暂不能加入实验设备

> **状态**：owner 2026-10-17 告知新进一台 A100，**公网 IP 149.165.152.232**，**现在还不能加入实验设备**。
> 在 owner 明确放行之前：不连接、不部署、不探测、不排进任何实验或 worker 池，也不计入容量。
> 它和 §2.4 存档的旧 a100（149.165.152.105，已退役）不是同一台。

| 项 | 值 |
|---|---|
| **公网 IP** | **149.165.152.232** |
| **tether 节点** | `a100`（2026-10-17 23:00 已 ONLINE） |
| **sudo** | **免密 sudo**（owner 2026-10-17）；只在确有需要时用，并记录每条命令 |
| **磁盘** | ⚠ **非常有限**（owner）：只拷测试必需的权重/运行时/输入，不留重复副本和大缓存，用完删自己建的文件 |
| **GPU / CPU / 内存** | 未登记（等价性验证时核实后补） |
| **当前用途** | **仅用于跨卡等价性验证**（owner 2026-10-17 23:0x 放行；astra 线程 01a14d26，`openpi/exp/offline_search/rounds/r17/xgpu_1017/`）；**仍不是正式实验设备**，不排进实验或 worker 池 |

---

### 2.9 h100-temp — 借用的单卡 H100 server（公网直连，2026-10-18 入网）

> ⛔ **借用机**：机器主人可能随时杀掉我们的进程。被杀后**不要重新跑**，停下该机队列并推通知给 owner 处理。
> 用法与 §2.7 h100 相同：公网直连，不 expose、不走 broker。

| 项 | 值 |
|---|---|
| **tether 节点** | `h100-temp` |
| **公网 IP** | **149.165.172.126**（主机名 megakernel；防火墙 ufw 未开，10-18 实测本机 / timan1 / timan107 都能直连 23170） |
| **用户** | `exouser`（与 h100 同名）；机器上还有别人的会话与项目，只动自己的目录 |
| **GPU** | 1 × NVIDIA H100 80GB HBM3（81 559 MiB），驱动 590.48.01 / CUDA 13.1（h100 是 595.x，版本不同） |
| **CPU / 内存** | 20 核 / 230 GiB |
| **系统** | Ubuntu 22.04.5（h100 是 24.04） |
| **磁盘** | 系统盘 993 GB，10-18 剩 174 GB；**无独立 /data 卷**，`/data` 是 10-18 用 sudo 建在系统盘上的目录（属主 exouser），路径与 h100 保持一致 |
| **sudo** | 可用；只在必要时用并记录（10-18：`mkdir /data && chown exouser /data`） |
| **端口** | 服务端口与 h100 相同的段（232xx / 231xx 由批次定义）；driver 在 weilandserver，h100-temp 队列用单独的 driver 端口，避免与 h100 队列冲突 |
| **当前用途** | R17 Fast-WAM MetaWorld 长段批次（与 h100 同型号，按 H100 定义的批次跑）；环境从 h100 同路径复制（`/data/mw_finetune/envs/fastwam`、`vendor`、`fastwam/mt50_frozen_b128/policy_016001`、`models/fastwam_libero`、`/data/xwam/wan22_5b`、`qualification/python_deps312`），传输用 h100 上临时只读 rsync daemon（23295，仅允许 149.165.172.126），传完关闭 |

## 3. tmux 会话命名规则

**所有设备统一**用 `srv0`, `srv1`, `srv2`, … 系列（不再按 client / server 分前缀）。

| 角色 | 前缀 | 示例 |
|---|---|---|
| **所有设备**（client / server 一致） | `srv` | `srv0`, `srv1`, `srv2`, … |

约定：
- 数字从 0 开始递增，每个新会话拿下一个未用过的编号；
- 长期常驻进程（policy server、模拟环境主循环、conductor 主跑）占低编号 `srv0`，
  临时调试 / 重跑 / 并行多 replica 用 `srv1` / `srv2` …；
- 创建：`tmux new -s srv0 -d`；进入：`tmux attach -t srv0`；列出：`tmux ls`；
- **输出流规约**：tmux 内运行命令时**不要**把所有输出全 redirect 到文件（`> log 2>&1`
  会让 attach 进去什么也看不到，断了人肉排错的最快路径）。**统一用 `tee` 落盘 +
  保留 stdout**，attach 进 pane 还能实时看到 server / worker 输出：

  ```bash
  # ✅ 正确：tee 既写文件又留 stdout，attach 可见
  tmux new -s srv0 -d "cd /path && <cmd> 2>&1 | tee /tmp/<srv>.log"

  # ❌ 错误：stdout 全丢文件，attach 进 pane 一片空白
  tmux new -s srv0 -d "cd /path && <cmd> > /tmp/<srv>.log 2>&1"
  ```

- 通过 tether 远程拉起会话的范式：
  ```bash
  tether exec <nid> -- bash -lc 'tmux has -t srv0 2>/dev/null || tmux new -s srv0 -d "cd <repo> && <cmd> 2>&1 | tee /tmp/<srv>.log"'
  tether run  <nid> -- bash -lc 'tmux attach -t srv0'    # 需要交互看输出
  ```

- 跨 tmux 镜像兼容（如 `jupyter-xuanlel2` 无系统 tmux）：用全路径
  `/home/<user>/miniforge3/bin/tmux` 或 `bash -lc` 进 conda env 后再调 `tmux`。

---

## 4. 对外暴露端口 —— 快速参考

### 4.0 优先级规则（2026-08-20 起）

对外暴露一个端口时，**按此顺序取**：

1. **weilandserver → 优先用直连段 `ziyanglin.com:23100-23199`**（§2.5.1）。少一跳 broker 转发、
   不占 broker 端口池、不受 yamux keepalive 长 idle 断流影响。前提：服务能监听在该段内（1:1 映射，不做端口转换）。
2. **该段被占满 / 服务无法改监听端口 / 其它节点** → 退回 `tether expose`（下方 §4.1）。
3. 有独立公网 IP 的节点（**h100 `149.165.153.233`**，端口段 23200-23299）→ 直连，两者都不用。
   h100 的主机防火墙与安全组实测全开（§2.7），服务只要 `--bind 0.0.0.0` 就对外可达。

查该段当前占用：`tether exec weilandserver -- bash -lc 'ss -tln | grep -E ":231[0-9][0-9]"'`

### 4.0b 实测链路带宽（2026-09-10）

| 链路 | 方向 | 实测 | 瓶颈 |
|---|---|---|---|
| timan108 ↔ h100（公网） | t108 → h100 | **117.2 MB/s（938 Mbit/s）** | timan108 的 **1 GbE** 网口 |
| timan108 ↔ h100（公网） | h100 → t108 | **117.2 MB/s（938 Mbit/s）** | 同上 |
| timan108 → h100，2 条并发流 | — | 58.9 + 58.8 = **117.7 MB/s** | 同上（并发不涨，确认是链路而非单流限制） |
| weilandserver → h100（公网 rsync） | 单流 | **31.1 MB/s**（200 GiB 实跑均值） | weilandserver 出口 |

测法：h100 侧监听（timan108 在校园 NAT `192.17.168.81` 后面，只能出不能入），两端各跑
`/tmp/bwtest.py`（15 s 定时打流，收端从第一个字节开始计时）。938 Mbit/s 是 1 GbE 线速的 93.8%，
基本打满，**再开并发流不会更快**。h100 的 virtio 网口不报速率（`/sys/class/net/enp1s0/speed` = -1），
但两个方向都能跑满 t108 的 1 G，说明它这一侧不是瓶颈。

⚠ 规划大数据搬运时用这两个数：走 timan108 约 **420 GB/h**，走 weilandserver 约 **112 GB/h**。

### 4.1 `tether expose` 反向隧道

broker 在 pc732（`weiland.top` → 155.98.36.32）。timan107 / jupyter 系列，以及 weilandserver 上
无法落进 23100-23199 的服务，通过 tether 反向隧道打到 broker 公网端口：

```bash
# 把 timan107 上的某个端口暴露到公网（broker=pc732/weiland.top）
tether expose timan107 --local 8000 --name sim-ui
# 输出形如：exposed: http://weiland.top:14001 → timan107:8000

# jupyter-ziyang10 同理
tether expose jupyter-ziyang10 --local 8888 --name jupyter

# 撤销
tether expose rm timan107 --name sim-ui
```

注意：
- `tether expose` 不带二次鉴权，敏感服务（pi0.5 推理端口、Jupyter）务必自带 token / TLS；
- 公网端口段固定 `14000-14999`，由 broker 自动分配，agent 重启后用持久化
  token 自动重连，URL 不变；
- 看当前已暴露的端口：`tether ps`（PORTS 节）。

---

## 5. 数据采集方式

本文档的硬件字段通过 tether 远程采集（采集日期 2026-05-23）：

```bash
tether exec <nid> -- bash -lc '
  lscpu | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\))"
  free -h | head -2
  nvidia-smi -L
  nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv
  # jupyter-ziyang10 额外读 cgroup 上限：
  cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/cpu.max
'
```

硬件变更（换卡、扩内存、cgroup 配额调整）后重跑上面命令并更新本文。
