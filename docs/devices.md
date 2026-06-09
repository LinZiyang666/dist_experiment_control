# 设备使用清单（openpi pi0.5 实验拓扑）

> 本文件记录当前 openpi pi0.5 实验所使用的 4 台设备：硬件配置、角色、
> openpi 项目路径、公网暴露策略，以及 tmux 会话命名规则。
> tether 操作细节请参考 `docs/usage.md`。

---

## 1. 拓扑总览

```
+---------------------+        policy 调用 ──►        +----------------------------+
|  timan107 (client)  |  ───────────────────────────►  | a100 (server)              |
|  NAT 后, 无公网 IP  |                                | 公网 149.165.152.105       |
+---------------------+                                +----------------------------+
          │
          │                              +---------------------------------+
          ├──────── policy 调用 ────────►| jupyter-ziyang10 (server)       |
          │                              | NAT 后, 无公网 IP, 资源受限     |
          │                              +---------------------------------+
          │
          │                              +---------------------------------+
          └──────── policy 调用 ────────►| jupyter-xuanlel2 (server)       |
                                         | NAT 后, 无公网 IP, 资源受限     |
                                         +---------------------------------+
```

- **client**：跑 openpi 的模拟环境，作为 policy 请求方；
- **server**：加载 pi0.5 权重，对外提供推理服务，被 client 调用。

4 台设备角色分工与 tmux 会话命名见下表 §2、§3。

---

## 2. 设备清单

### 2.1 timan107 — 模拟环境 client

| 项 | 值 |
|---|---|
| **角色** | client（运行 openpi 模拟环境，发起 policy 请求） |
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

### 2.4 a100 — pi0.5 server（公网入口）

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

---

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

## 4. tether 公网暴露快速参考

broker 在 pc732（`weiland.top` → 155.98.36.32）。前两台（timan107 / jupyter）要对外提供服务
（debug HTTP、Jupyter、远程客户端连推理端口等）时，通过 tether 反向隧道打到 broker 公网端口：

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
