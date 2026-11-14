# 多中心患者编号对照系统

多中心研究场景下的数据工程工具：归并各医院院内患者编号，维护"同人关系"并自动
形成研究取数用的编号对照表。基于 **Flask + SQLite（WAL）**，后台异步分组计算，
提供可演示的 Web 入口与 Docker Compose 一键启动环境。

## 一、快速启动

### 方式 1：Docker Compose（推荐交付方式）

```bash
docker compose up --build -d
# 浏览器打开 http://localhost:8000
docker compose logs -f          # 查看日志
docker compose down             # 停止；数据保留在命名卷 patient_data 中
```

### 方式 2：本地 Python 运行

```bash
pip install -r app/requirements.txt
./run_local.sh                  # 或：gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 app:app
# 浏览器打开 http://localhost:8000
```

数据默认写入 `./data/`（`patients.db` + `uploads/`），可用环境变量
`APP_DATA_DIR` / `APP_DB_PATH` / `APP_UPLOAD_DIR` 覆盖。

## 二、演示路径（约 3 分钟）

1. 首页上传 `demo/batch_a.csv`，再上传 `demo/batch_b.csv`
   （后者含重传键 `H01/MRN-1001`，页面可见"复用"计数，记录号不重复分配）。
2. 进入 **核对关系** 页，用记录号确认同人，例如 `1 ↔ 3`、`3 ↔ 5`
   （传递成组，组号取组内最小记录号）。
3. 尝试冲突确认：对同组已含某医院编号的组，再确认该医院的另一编号会被拒绝，
   页面显示具体拒绝原因。
4. 撤销一条关系：若剩余关系仍有路径相连则不拆组，否则按剩余关系重新拆组。
5. **分组与下载** 页查看每个组的成员，下载 `patient_mapping.csv`
   （响应头 `X-Compute-Id` 标注本次下载对应的完整状态版本）。
6. 首页可查看任务进度条、失败任务与拒绝原因。

CSV 格式：表头必须包含医院列（`hospital`/`医院`）与院内编号列（`mrn`/`院内编号`），
支持 UTF-8（含 BOM）。示例见 `app/static/sample_patients.csv`。

## 三、需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 上传患者 CSV（医院、院内编号），组合唯一，递增记录号，同键重传复用 | `patients` 表 `UNIQUE(hospital, hospital_mrn)` + 自增主键；导入任务逐行 upsert，统计新增/复用 |
| 核对员确认、撤销两条记录的同人关系 | `/relations/confirm`、`/relations/revoke`；关系以无向边规范化（小 id 在前），ACTIVE 部分唯一索引防重 |
| 关系传递成组，组号取最小记录号 | 并查集求连通分量（`app/groups.py`），组号 = 分量内最小记录号 |
| 同组不得有同医院不同编号 | 每次计算逐组检测同医院多编号，冲突则整次计算 FAILED、**正式分组保持上一版不变**，原因入任务可查 |
| 撤销按剩余关系拆组，仍有其他路径不拆 | 撤销仅把边置 REVOKED，重算基于剩余 ACTIVE 边重建分量 |
| 后台计算，期间仍可导入/核对/查询，计算不占库写事务 | 后台守护线程；计算阶段在只读快照取数后即释放连接，并查集纯内存运算；仅发布阶段用一个短写事务 |
| 并发确认按提交时完整关系判定；合并违规的两次确认只成功一次，无关确认保留 | 所有写事务 `BEGIN IMMEDIATE` 串行化，确认在同事务内基于完整 ACTIVE 关系构建分量并模拟合并校验 |
| 关系、组成员、组号一起生效；迟到计算不恢复旧分组 | 发布在单事务内整体替换 `groups`；任务带关系序列号 `seq_cutoff`，计算/发布前比对最新序列，过期即作废并补排最新任务 |
| 每次查询或下载来自一个完整状态 | 所有读操作在单个只读快照事务内完成；下载带 `compute_id` 版本标识 |
| 失败不改正式分组 | 冲突/过期只更新任务状态，不触碰 `groups` |
| 重启保留成功变动，未完成操作标失败 | WAL 持久化；新后台领导当选时把前任遗留在 RUNNING 的任务标 FAILED 并注明"后台进程退出"，PENDING 排队任务保留并继续执行 |
| 平滑重载期间全局只执行一个导入/重算 | 数据目录内 `worker.lock` 进程级排他咨询锁（`fcntl.flock`）：仅持锁进程的后台线程认领任务；非领导进程只提供 Web 能力 |
| 存活（含暂停）旧进程不被误报失败、不重复执行 | 锁绑定持锁进程生命周期，SIGSTOP 不释放锁；任务认领登记 `worker_id`，所有回写带 `worker_id + status='RUNNING'` 所有权围栏 |
| 新 worker 不等旧任务结束即可服务 | Web 路由不依赖领导锁；未获锁进程的上传/核对/进度/下载立即可用，任务由持锁进程统一执行 |
| 页面可查进度、成员、拒绝原因，下载对照 CSV | 三个页面 + 任务进度条 + 失败原因区 + `/download/mapping.csv` |

## 四、架构说明

```
app/
├── config.py       # 路径与轮询配置（环境变量可覆盖）
├── db.py           # 连接/Schema、WAL、IMMEDIATE 写事务、只读快照、幂等迁移
├── leader.py       # 跨进程后台领导锁（fcntl.flock，worker.lock）
├── groups.py       # 并查集、连通分量、同医院冲突检测（纯函数）
├── services.py     # 导入入队、CSV 解析、确认/撤销的完整关系判定
├── worker.py       # 后台线程：领导竞选、导入逐行入库、重算、原子发布、退出交接
├── repository.py   # 只读查询：统计/进度/成员/失败原因/对照 CSV
├── web.py          # Flask 路由
├── templates/      # 三个页面（导入与进度 / 核对关系 / 分组与下载）
└── static/         # 示例 CSV
tests/
├── test_groups.py            # 算法纯函数
├── test_integration.py       # 进程内端到端、并发、围栏与重启恢复
├── test_process_handoff.py   # 独立操作系统进程：启动重叠、暂停恢复、退出交接
├── process_harness.py        # 上者使用的子进程引导脚本
└── _pylibs/                  # 独立进程测试的依赖目录（本地生成，已 gitignore）
demo/               # 演示用 CSV
```

### 平滑重载与后台交接

Gunicorn 单 worker 平滑重载（SIGHUP、`--max-requests` 滚动重启）期间，
新旧进程会并存并共用同一数据目录。本系统用 **进程级领导锁 + 任务所有权
围栏** 保证交接正确：

1. 每个进程启动后台线程时先竞争数据目录内的 `worker.lock` 排他咨询锁。
   锁与持锁进程生命周期绑定：正常退出随描述符关闭释放，崩溃/被 KILL
   由内核自动释放；被 SIGSTOP 暂时暂停时锁不释放，不会误触发交接。
2. 只有持锁进程（leader）认领并执行任务，全局任一时刻只有一个导入或
   重算在跑；未获锁进程不接触任务队列，但其上传、核对、进度查询与完整
   对照下载立即可用，无需等待旧任务结束。
3. 任务认领即写入本进程的 `worker_id`；之后进度、失败、发布、完成等
   所有回写都带 `worker_id + status='RUNNING'` 条件，旧进程无法回写
   已终结任务，也无法覆盖新进程发布的分组（发布版本号 `compute_id`
   单调递增）。
4. 新 leader 当选说明前任已退出：仅把前任遗留在 RUNNING 的任务标
   FAILED（已逐行提交的患者、已发布分组随 WAL 保留），PENDING 排队
   任务原样保留并由新 leader 接着执行，重载不丢队列。

> 部署约束：不要给 Gunicorn 加 `--preload`，否则锁文件描述符会在
> fork 时被复制，破坏"每进程独立竞选"的语义。

### 并发与一致性模型

- **WAL 模式**：读写不互斥；写事务由 SQLite 全局串行化，配合 `busy_timeout`
  与应用层重试。
- **确认/撤销事务**：`BEGIN IMMEDIATE` 立即取写锁，后到的并发确认在同一完整
  关系状态上重新校验，因此"各自合法、合并不合法"的两个确认只有先提交者成功。
- **分组生命周期**：
  1. 关系变动在同一写事务内入队 RECOMPUTE（携带当前最大关系序列号）；
  2. worker 在只读快照中取数（读完即释放），事务外完成并查集计算；
  3. 发布前比对序列号，过期则作废并补排；检测到冲突则 FAILED 且不改分组；
  4. 通过后在单写事务内整体替换 `groups`，关系/成员/组号原子生效。
- **任务合并**：已有 PENDING 重算时只更新序列号，避免高频确认造成任务风暴。

## 五、测试

```bash
pip install -r app/requirements.txt pytest
python3 -m pytest tests/ -v
```

覆盖：递增编号与同键复用、传递成组与最小组号、撤销拆组/替代路径保留、
同医院冲突检测、并发确认只成功一次且无关确认保留、幂等重复确认、
迟到计算作废、计算失败不覆盖正式分组、所有权围栏（旧 worker 无法回写
已终结任务）、重启恢复、HTTP 上传与快照下载。

独立进程交接测试（`tests/test_process_handoff.py`）以 `python -I`
拉起真实操作系统进程，用文件门控精确控制启动重叠、SIGSTOP/SIGCONT
暂停恢复与进程退出交接。子进程在隔离模式下不读取 `PYTHONPATH` 与用户
site-packages，需先把依赖装入仓库内的测试目录：

```bash
pip install --target tests/_pylibs -r app/requirements.txt pytest
python3 -m pytest tests/test_process_handoff.py -v
```

四个场景：① 启动重叠时存活任务不被误报失败、不被重复执行，新进程
Web 立即可用；② 暂停的持锁进程不被备用进程接管，备用进程持续提供
页面服务；③ 旧进程退出后 RUNNING 任务标失败、已提交患者与成功分组
保留、PENDING 不丢失并被接手；④ 备用进程完整支持上传、核对、进度与
带 `X-Compute-Id` 的对照下载。

## 六、运维备注

- 容器以单 gunicorn worker + 多线程运行（后台线程与 Web 同进程协作，
  不使用 `--preload`）；数据存于命名卷 `patient_data`，
  `docker compose down` 不删数据，`docker compose down -v` 才会清除。
- 健康检查：`GET /healthz`。
- 上传大小上限 32MB（`web.py` 可调）。
- 领导锁文件位于数据目录内的 `worker.lock`（可用 `APP_LEADER_LOCK_PATH`
  覆盖）；多个进程共用同一数据目录时，只有一个进程的后台线程执行任务，
  其余进程保持 Web 可用并等待接管。
