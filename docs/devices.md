# 设备使用清单（openpi pi0.5 实验拓扑）

> 本文件记录当前 openpi pi0.5 实验所使用的设备：硬件配置、角色、
> openpi 项目路径、公网暴露策略，以及 tmux 会话命名规则。
> tether 操作细节请参考 `docs/usage.md`（使用者）、`docs/broker-ops.md`（broker 运维）、`docs/cluster.md`（集群 HA）。
>
> 更新记录：2026-08-13 增补 §2.5 weilandserver（自给自足 server+client 节点）与 §2.6 未配置节点。
> 2026-08-20 weilandserver 获得**直连公网入口** `ziyanglin.com:23100-23199`（交换机 NAT），对外暴露的优先级规则见 §4.0，实测记录见 §2.5.1。

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
          ├──────── policy 调用 ────────►| jupyter-xuanlel2 (server)       |
          │                              | NAT 后, 无公网 IP, 资源受限     |
          │                              +---------------------------------+
          │
          │                              +---------------------------------+
          └──────── policy 调用 ────────►| weilandserver (server + client) |
                                         | ziyanglin.com:23100-23199 直连   |
                                         | (交换机 NAT; 4090 48G)           |
                                         | 本机自带 LIBERO 模拟环境        |
                                         | → 可单机闭环, 见 §2.5           |
                                         +---------------------------------+
```

- **client**：跑 openpi 的模拟环境，作为 policy 请求方；
- **server**：加载 pi0.5 权重，对外提供推理服务，被 client 调用。
- **自给自足节点**：weilandserver 两个角色都能担（client 走 `127.0.0.1` 直连本机 server，不经 broker），
  适合不需要横向扩 client 的中小规模 rollout；大规模并发仍用 timan107 车队当 client。

设备角色分工与 tmux 会话命名见下表 §2、§3。

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

> **⚠ 状态（2026-08-13 实测）**：`tether node ls` 中**没有 a100**，该节点当前不在线；
> 下表为最后一次在线时的配置，重新启用前需确认机器与 agent 状态。

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
| **openpi 目录** | `/home/weiland/openpi`（uv venv `.venv`，python 3.11） |
| **uv 二进制** | `/home/weiland/.local/bin/uv` |
| **模型权重** | `/home/weiland/.cache/openpi/openpi-assets/checkpoints/pi05_libero_pytorch` |
| **lerobot 环境** | venv `/home/weiland/lerobot_venv`（+ conda env `lerobot`），用于 SmolVLA / ACT sidecar |
| **LIBERO 环境** | **`/home/weiland/libero_sim`**（conda prefix，python 3.8.20；2026-08-13 按 timan107 配方复刻，见下方“LIBERO 环境”小节） |
| **conda** | `/home/weiland/miniconda3/bin/conda`（26.5.3） |
| **tether agent** | 用户态进程 `tether agent --session lab --nid weilandserver`（非 systemd 服务） |
| **tmux 会话前缀** | `srv0`, `srv1`, `srv2`, …（现役还有 `sml*` / `sm*` / `acts*` 系列 sidecar 会话，来自 ablation 实验） |

**⚠ GPU 稳定性（2026-08-20 实测判定，必读）**：这张 48G 改装 4090 在**出厂功率/频率下不稳定**——gpu_burn 600s 判 `FAULTY`：冷卡拉满载后的 **28–62 s 爬坡窗口**内产生 4,069 万个**静默计算错误**（67–71 °C，非过热，零 Xid 零报错），窗口外干净。同一不稳定曾以 `Xid 31 MMU Fault @ 0x0` 形态三次打死推理 server（都在起流量后 ~3 min）。**缓解（已验证）**：

**⚠ 已废弃的旧缓解**：降功率+锁频（`-pl 350` + `-lgc`）只能救稳态 GEMM，救不了推理负载（降压状态下推理照崩两次），不要再用。

**✅ 现行缓解（同日六轮实验验证）：保温协议。** 判定依据：冷启动 ≤36 °C 满带宽负载 **3/3 全灭**（40.7M 错误 / 2.1M 错误 / 静默算死挂起——第三种死法只有 `proc'd` 计数冻结可见，`errors: 0` 是僵尸读数）；起温 ≥44 °C **2/2 全过**。故障 = 冷态陡热爬坡 × 满带宽显存流量两条件叠加；单独任一不触发。

**强制规程（本卡跑任何 GPU 负载前必须遵守）**：

1. **实验/负载启动前**，先起保温脚本并确认 `engage` 后温度 ≥50 °C：
   ```bash
   tmux new -s keepwarm -d "cd /home/weiland/openpi && /home/weiland/.local/bin/uv run python /home/weiland/gtp_logs/gpu_keepwarm.py 2>&1 | tee -a /home/weiland/gtp_logs/keepwarm.log"
   ```
2. **保温脚本是常驻服务，不随实验结束关闭**（owner 裁定 2026-08-20：该缺陷与具体实验无关，任何冷态起满载的负载都会撞上，后续其它工作同样依赖它）。实验中途重启 conductor/server 的窗口正是最危险的冷却窗口，更不能停。只有明确要让卡彻底冷下来时（送修 / 拆机 / 长期停用）才 `tmux kill-session -t keepwarm`。
   ⚠ **重启后不会自动恢复**：tmux 会话随重启消失，而冷启动恰恰是最危险的窗口。物理或软重启之后的**第一件事**就是重挂保温并确认 ≥50 °C，之后才允许起任何 GPU 负载。
3. 脚本设计（`/home/weiland/gtp_logs/gpu_keepwarm.py`，v6 定稿）：**纯恒温器,温度是唯一输入**（owner 裁定 2026-08-20：不看进程/利用率——轻负载进程也可能让温度跌破线,那时就该并行加热；对重负载的不干扰由物理保证:重负载自己把温度顶在介入线上,保温自然休眠）。常驻 **~432 MiB**（2×1536² fp32 张量 ~18 MB + 钳制的 cuBLAS workspace + 不可避免的 CUDA context，实测 `nvidia-smi` 432 MiB）；恒温带 **54 °C 介入 / 62 °C 完全让出**（零 kernel）；≤60 °C 近带区**紧轮询**（~0.5 s）——负载骤停后结温 2 秒自由落体 10 °C,常规轮询接不住（v1 实测谷值擦到 44 线）；实测 25 循环谷值 50–53 °C,距安全线 6 °C 余量。
4. **wedge 后软重启不可靠**：GPU 卡死后 `systemctl reboot` 可能被 D 状态进程 + GSP RPC 串行超时（每个 75 s）钉死在半关机态（2026-08-20 实测一次，节点 OFFLINE 但不重启，需物理按键）。规程：软重启下发 5 分钟未回 ONLINE = 关机挂死，直接物理重启；物理断电复位对 GSP 反而更彻底。
5. 判 gpu_burn 结果必须看三件套：`errors` + **`proc'd` 是否持续推进** + 终判行——只看 errors 会被静默挂死骗过（2026-08-20 实测被骗一次）。

判定与全部日志：`weiland-wsl:/mnt/c/Users/lzy66/Downloads/gtp_logs/`（送修报告 `REPAIR_REPORT.md`）+ 本机 `/home/weiland/gpu_fault_reports/`、`/home/weiland/gtp_logs/`。**该卡在送修前不承担要求结果可信的正式实验**（静默算错前科 + 当日退化轨迹）。

**GPU 备注**：4090 是**独占卡**（util 与 memory 都是真实信号，不像 jupyter 系列共享卡只能看 `memory.free`）。
显存 48 GiB 可同时驻多个 pi0.5 server + 若干小模型 sidecar；起新负载前照例先 `nvidia-smi` 查 `memory.free`。

**LIBERO 环境（2026-08-13 新建，与 timan107 同配方）**：

| 项 | timan107 | weilandserver |
|---|---|---|
| prefix | `/scratch/zixuans8/libero_sim` | `/home/weiland/libero_sim` |
| python | 3.8.20 | 3.8.20（一致） |
| 关键包 | libero 0.1.1 / robosuite 1.4.0 / mujoco 3.2.3 / bddl 1.0.1 / robomimic 0.2.0 / numpy 1.22.4 / torch 1.11.0+cu113 | 同左（按 timan107 `pip freeze` 逐版本 `--no-deps` 锁定，共 126 包） |
| `openpi-client` | editable → `/scratch/zixuans8/openpi/packages/openpi-client` | editable → `/home/weiland/openpi/packages/openpi-client` |
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
  cd /home/weiland/openpi
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
| `timan1` | 48 logical CPU / 503 GiB RAM / **4 × RTX A6000（各 48 GiB）** | 车队里最大的一块未开发算力 |
| `timan108` | 48 logical CPU / 251 GiB RAM / **3 × RTX A5000（各 24 GiB）** | 未开发 |
| `weiland-optiplex-7050` | 小型机 | 跑 daemon（`daemon-console:14060` / `daemon-gw:14766`），非计算节点 |
| `racknerd` | 1 vCPU VPS | 网络中转，非计算节点 |
| `weiland-wsl` | 开发机（WSL2） | 本地 repo + 实验原始数据主副本，非计算节点 |

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

## 4. 对外暴露端口 —— 快速参考

### 4.0 优先级规则（2026-08-20 起）

对外暴露一个端口时，**按此顺序取**：

1. **weilandserver → 优先用直连段 `ziyanglin.com:23100-23199`**（§2.5.1）。少一跳 broker 转发、
   不占 broker 端口池、不受 yamux keepalive 长 idle 断流影响。前提：服务能监听在该段内（1:1 映射，不做端口转换）。
2. **该段被占满 / 服务无法改监听端口 / 其它节点** → 退回 `tether expose`（下方 §4.1）。
3. 有独立公网 IP 的节点（a100）→ 直连，两者都不用。

查该段当前占用：`tether exec weilandserver -- bash -lc 'ss -tln | grep -E ":231[0-9][0-9]"'`

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
