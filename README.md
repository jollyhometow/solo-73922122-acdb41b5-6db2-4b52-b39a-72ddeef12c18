# 港口地磅称量流 · 净重明细服务

把地磅称量消息（箱号、毛重、皮重，整数千克）整理成**净重明细**入仓；
字段无效或毛重小于皮重的消息进入**隔离区**，可查看原文并**补正**后入仓。

技术栈：**FastAPI + PostgreSQL + 本地 Redpanda（Kafka 协议）+ Docker Compose**，
两个 API 实例以**同一消费组**消费**两个分区**。

---

## 1. 一键启动

```bash
docker compose up -d --build
```

启动后：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 实例 1 / Swagger | http://localhost:8001/docs | 双实例之一 |
| API 实例 2 / Swagger | http://localhost:8002/docs | 双实例之二 |
| Redpanda Console | http://localhost:8080 | 查看 Topic、分区、消息 |
| PostgreSQL | localhost:5432 | 库 `weighbridge`，用户/密码 `weigh/weigh` |
| Redpanda Kafka 端口 | localhost:19092 | 宿主机可用 Kafka 客户端连接（容器内服务走 `redpanda:9092`） |

健康检查：

```bash
curl http://localhost:8001/health
# {"status":"ok","instance_id":"instance-1","consumer_enabled":true,"assigned_partitions":[...]}
```

两个实例同属消费组 `weighbridge-consumer-group`，Topic `weighbridge-events` 有 2 个分区，
因此稳态下每个实例各消费一个分区（重平衡后由组协调器重新分配）。

---

## 2. 在 /docs 走一遍演示流程

也可以直接用命令行：

```bash
# 1) 一键投递 6 条演示消息（2 合法 + 4 各类非法）
curl -X POST http://localhost:8001/demo/seed

# 2) 发送单条合法消息
curl -X POST http://localhost:8001/messages -H 'Content-Type: application/json' -d '{
  "message": {"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000}
}'

# 3) 发送一条会被隔离的消息（毛重 < 皮重）
curl -X POST http://localhost:8001/messages -H 'Content-Type: application/json' -d '{
  "message": {"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200}
}'

# 4) 查询净重明细
curl http://localhost:8001/net-weights

# 5) 查询隔离记录（可按分区/状态过滤）
curl "http://localhost:8001/quarantine"
curl "http://localhost:8001/quarantine?partition=0&status=OPEN"

# 6) 按源分区 + offset 查看隔离原文
curl http://localhost:8001/quarantine/by-position/0/1

# 7) 对隔离记录补正（把 <隔离记录 id> 换成实际 id）
curl -X POST http://localhost:8001/quarantine/1/correction \
  -H 'Content-Type: application/json' \
  -d '{"container_no":"MSCU1234567","gross_weight":5000,"tare_weight":4200}'
```

### 接口清单

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/messages` | 投递演示消息（支持畸形 JSON 原文 `raw_text`，可指定分区） |
| POST | `/demo/seed` | 一键投递一组合法/非法演示消息 |
| GET | `/net-weights` | 查询净重明细，可按箱号过滤 |
| GET | `/quarantine` | 查询隔离记录，可按源分区、状态过滤 |
| GET | `/quarantine/by-position/{partition}/{offset}` | **按源分区与 offset 查看隔离原文** |
| GET | `/quarantine/{id}` | 按主键查单条隔离记录 |
| POST | `/quarantine/{id}/correction` | 提交补正（每条仅接受一次合法补正） |
| GET | `/health` | 健康检查与当前分配分区 |

---

## 3. 数据模型

### `net_weight_records`（净重明细，只追加）

- 业务字段：`container_no`、`gross_weight`、`tare_weight`、`net_weight = gross - tare`
- 来源定位：`topic`、`source_partition`、`source_offset`
- `from_correction`：是否来自隔离补正；`quarantine_id`：关联的隔离记录
- **唯一约束 `(topic, source_partition, source_offset)`**：每条源消息最多入仓一次
- 约束：毛重/皮重非负、毛重 ≥ 皮重、净重非负

### `quarantine_records`（隔离记录）

- `raw_payload` 原样保留消息原文；`reason_code` / `reason_detail` 记录错误原因
- 同样以 `(topic, source_partition, source_offset)` 唯一约束，**同文不同位置分别保留**
- 状态机：`OPEN` → `CORRECTED`（只允许翻转一次）
- `correction_attempts`、`last_correction_error` 记录失败补正（失败保持 `OPEN`）

---

## 4. 正确性设计（如何满足每条要求）

### 4.1 每条源消息仅入仓一次、不重复明细

- 消费处理先做幂等前置检查，再以数据库**唯一约束兜底**并捕获 `IntegrityError`，
  任何重放/重复投递都走 `DUPLICATE_SKIP`，不会产生第二条明细。
- 净重表与隔离表各自持有来源位点唯一约束。

### 4.2 持久成功后才推进消费位点

消费者关闭自动提交（`enable.auto.commit=false`、`enable.auto.offset.store=false`），
每条消息严格按序执行：

1. 数据库事务提交成功（明细落库 **或** 隔离落库）；
2. `store_offset` 仅在内存标记位点；
3. 同步 `commit` 把位点提交给 Redpanda。

任一环节失败都不会推进位点。

### 4.3 写库失败 / 提交断连 / 实例退出 / 重平衡后接续，不漏不重

- **写库失败**：回滚事务、不提交位点，`seek` 回当前消息重试（带指数退避）。
- **提交位点时断连**：捕获异常后 `seek` 回本条消息；重放时唯一约束识别为重复并跳过，再正常提交。
- **实例退出 / 分区重分配**：未提交的位点不会被确认，新 owner 从 Broker 已提交位点重放，
  由幂等键去重——因此**不丢消息、不重复明细**。
- 静态成员 `group.instance.id` + 协作式重平衡（`cooperative-sticky`）减少不必要的分区抖动。

### 4.4 迟到消费不能覆盖补正结果

补正成功后，该来源位点在净重表已有记录。迟到/重放消息在前置幂等检查时即被识别为
`DUPLICATE_SKIP`，**不会再走校验或写库分支**，补正生成的明细与 `CORRECTED` 状态不受影响。

### 4.5 并发补正仅一份生效，失败保留隔离状态

- 补正在独立事务中以 `SELECT ... FOR UPDATE` 锁定该隔离记录（PostgreSQL 行锁），串行化并发请求。
- 持锁事务把状态从 `OPEN` 翻转为 `CORRECTED` 并插入明细；后到者读到 `CORRECTED` 直接返回 `409 ALREADY_CORRECTED`。
- 净重表来源位点唯一约束是第二道防线，任何情况下都不会插入两条明细。
- 非法补正只累加 `correction_attempts`、记录错误并提交，**隔离状态保持 `OPEN`**，不影响后续合法补正。

### 4.6 字段校验规则（消费与补正同一套口径）

- 消息体必须是 JSON 对象；
- 箱号非空字符串，长度 4–17，仅字母/数字/连字符；
- 毛重、皮重必须是**严格整数**（布尔、浮点、字符串数字一律拒绝），非负且不超上限；
- 毛重 ≥ 皮重，否则以 `GROSS_LT_TARE` 隔离；毛重 = 皮重合法（净重 0）。

---

## 5. 本地开发与自测

不依赖 Docker 也能跑测试：消息层抽象了 `Broker` 接口，测试用进程内 `InMemoryBroker`，
数据库用临时文件 SQLite（WAL），完整覆盖 API → 后台消费者 → 入库/隔离/补正链路。

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install pytest httpx
pytest -q
```

> 说明：测试环境无 Docker/PostgreSQL/Redpanda。SQLite 的 `FOR UPDATE` 为空操作、
> 单文件库无法复现真实多连接行锁，因此“多线程并发补正”用例以 `xfail` 标注其平台限制，
> 并发安全的**确定路径**由顺序锁状态机用例 + PostgreSQL 行锁/唯一约束设计共同保证。

### 无 Redpanda 时本地直接运行 API（内存 Broker）

```bash
export DATABASE_URL="sqlite:////tmp/weighbridge.db"
export BROKER_BACKEND=memory
uvicorn app.main:app --reload --port 8000
# 打开 http://localhost:8000/docs
```

---

## 6. 代码结构

```
app/
  config.py            # 环境变量配置
  database.py          # 引擎/会话/建表
  models.py            # 净重明细、隔离记录 ORM（含唯一约束/检查约束）
  validation.py        # 纯函数消息校验（各类隔离原因）
  schemas.py           # HTTP 请求/响应模型
  messaging.py         # Broker 抽象：Kafka(redpanda) 实现 + 内存实现
  consumer.py          # 消费工作线程：先落库后提交位点、失败 seek 重试
  services.py          # 幂等入仓、隔离、补正状态机、查询
  main.py              # FastAPI 入口与路由
tests/                 # 校验/服务/端到端/消费者可靠性测试
scripts/entrypoint.sh  # 容器入口：等待依赖后启动 uvicorn
docker-compose.yml     # postgres + redpanda + topic-init + app-1 + app-2 + console
Dockerfile
```

---

## 7. 已实现 / 已验证 / 未完成

### 已实现

- 双实例同组消费双分区、FastAPI 全套接口、Swagger 可直接发演示消息/补正/查询；
- 严格整数千克校验、毛重<皮重等各类无效消息隔离并保留原文与原因；
- 按源分区 + offset 查隔离原文；同文不同位置分别保留；
- 补正状态机（行锁 + 唯一约束），每条仅一次合法补正，失败保持隔离；
- “先持久化后提交位点”、失败 seek 重试、重放幂等去重、迟到消息不覆盖补正；
- 可启动全部服务的 Docker Compose（含健康检查、Topic 初始化、Redpanda Console）。

### 已验证（本地，无 Docker 守护进程）

- 28 项测试通过、1 项平台相关 xfail，多轮重复运行稳定；
- 真实 uvicorn 进程 HTTP 冒烟：投递→入仓/隔离→非法补正被拒→合法补正→重复补正 409；
- 全部模块（含真实 `confluent-kafka`、`psycopg2`）可导入，compose YAML 与入口脚本语法校验通过。

### 未完成 / 需在目标环境验证

- 当前沙箱**没有 Docker 守护进程与 PostgreSQL/Redpanda 服务**，
  无法在本机实际 `docker compose up` 验证容器编排与真实 Kafka 行锁行为；
  相关路径已通过接口抽象、配置与静态检查保证，建议在有 Docker 的环境执行一次第 1、2 节流程确认。
- 建表演示用 `create_all`，生产可替换为 Alembic 迁移。
