# MQTT 设备数据入数仓 — 面试实现详解（代码核实版 v3）

> **定位**：补齐简历中"设备 MQTT 上传 → 数仓"链路的实现级细节。本文所有结论基于 `G:\Project\goodwe` 真实代码审计（2026-08-27 多轮核实），分"✅ 已坐实 / ⚠️ 简历描述 / ❌ 与代码矛盾 / 🔶 源码未在本 checkout"四档标注。
>
> **⚠️ 关键认知（决定你怎么理解本文的"未核实"）**：`G:\Project\goodwe` 只是 goodwe 整个仓库的**本地子集**——`iot`/`middle`/`project`/`baobi` 多模块拼装，但 nacos 配置导出里还有大量 `secp-iot-*`、`middle-*`、`secp-message-convert` 等服务的 dataId，源码并不在你这边。所以本文里 **🔶 标注的环节不是"项目没做"，而是"你本地 checkout 没拉全"**——nacos 配置就是它们真实存在的铁证，简历那条"设备 MQTT → 数仓"链路是可信的；面试讲细节时只需避免拍胸口说"我亲手改过那块代码"。
> **配套文档**：《面试题_简历驱动版.md》第七章、《Java基础面试_固德威业务场景版.md》第十二章（MQTT 协议）、6.6 节（TimescaleDB DDL）。
> **一句话口径（面试先亮这个）**：设备数据经**边缘采集(Rust)→ HTTP → 边缘 data-manager 缓冲 → 接入层(源码未在本 checkout)→ Kafka `secp-rich-*-event-data`（数仓 topic）→ `secp-prophet` 消费 → ClickHouse（这才是"数仓"）**；另一条支线：Kafka 告警/状态 topic → `secp-watchman` → 5min 曲线 API → `secp-algorithm` 定时调度拉取 → **PG + TimescaleDB 的 `_t0/_t1` 聚合曲线超表**（这是聚合产物，不是原始数仓）。**简历里的 Flink 不存在**（全仓库零 `org.apache.flink`），真实是 Spring Kafka + 定时调度；**边缘出门是 HTTP 不是 MQTT**。

---

## 一、全链路总览（代码核实版）

```
设备（逆变器/储能柜/采集器 EzLogger/SEC 系列，Modbus/IEC104）
  │  Rust 边缘采集（sebu-edge-collector）：读寄存器 → formula 引擎换算为物理量
  ▼
边缘 data-manager（sebu-edge-data-manager，Rust+actix-web）
  │  HTTP POST /realtime_data_batch（task_manager.rs:362 send_by_service）
  │  本地缓冲：SQLite 按天分库(tsdb_YYYYMMDD.db) + 无锁实时缓存(DashMap)
  ▼
【设备接入/转发层：secp-iot-* / secp-message-convert / middle-forward】
  │  🔶 源码未在本 checkout（仅 nacos 配置泄漏其存在），负责把设备数据送入 Kafka
  ▼
Kafka（总线，Spring Kafka 消费，非 Flink）
  ├─▶ topic `secp-rich-*-event-data`（data-warehouse-kafka，数仓 topic）
  │        ▼  secp-prophet @KafkaListener（DWStatus/DWEvent/...MessageDataSource）
  │        ▼  MessageRecordRepository → ★ ClickHouse（这才是"数仓"：设备事件/状态/运行记录）
  │
  └─▶ topic `prophet-alarm / prophet-notify / device-communicate-status-change`
           ▼  secp-watchman @KafkaListener（告警/状态/通知）
           ▼  watchman 提供 5min 曲线 API（getStationMetricData）
                     ▼  secp-algorithm 定时调度(@Async generateRt5MinCurves)
                     ▼  fetchStationMetricInfo → watchmanService 拉曲线 → saveBatch
           ★ PG + TimescaleDB 的 _t0/_t1 聚合曲线超表（聚合产物，非原始数仓）
                              │
                              ▼
              secp-biz-data 统一指标查询（T+0 实时 / T+1 离线）
                              ▼
          secp-watchman(监控) / secp-screen(大屏) / 报表
```

**各层职责一句话**：
- **边缘 iot（Rust）**：协议解析 + 边缘计算(formula 把裸寄存器换算成物理量) + 本地缓冲。出门走 **HTTP**，不是 MQTT。✅ 已坐实
- **接入/转发层（secp-iot-* 等）**：🔶 设备 MQTT→Kafka 的第一跳，源码未在本 checkout
- **Kafka**：削峰、解耦、一份数据多方消费。真实客户端是 **Spring Kafka**，没有 Flink。✅ 已坐实
- **secp-prophet + ClickHouse**：**真正的"数仓"**——消费 Kafka dwh topic，存设备事件/状态/运行海量记录。✅ 已坐实
- **secp-watchman**：消费告警/状态 topic，提供实时曲线查询 API。✅ 已坐实
- **secp-algorithm + PG/TimescaleDB**：定时调度从 watchman 拉曲线，写 `_t0/_t1` 聚合超表。✅ 已坐实（写入方是算法服务，不是 Flink）
- **secp-manager**：MQTT 入站只接 `operationlog/frontend`（操作日志/前端消息）+ Spring Kafka 消费业务消息（设备-产品关系、用户、操作日志）。✅ 已坐实

---

## 二、边界话术（先说这个，防追问跑偏）

> "我们不是传统 Hive 离线数仓，是**实时管道 + 双存储**路线：设备遥测经边缘采集和接入层进 Kafka，告警/事件类数据由 `secp-prophet` 落 ClickHouse 做数仓分析；同时一份数据经 watchman 进 `secp-algorithm`，定时聚合写 PG/TimescaleDB 的超表做曲线查询。ClickHouse 和 TimescaleDB 是两条独立链路，不是同一份数据的双写——设备事件进 ClickHouse，聚合曲线进 TimescaleDB。"

被问"Flink 在哪"：**坦诚说算法服务的实时聚合是用 Spring Kafka 消费 + 定时调度（`@Async`）实现的，没有独立 Flink 集群**——这比硬编 Flink 作业名稳。

---

## 三、边缘侧 iot（Rust 采集端）— 出门是 HTTP，不是 MQTT

- 模块：`sebu-edge-collector`（主采集，子 crate modbus/iec104/interface）、`sebu-edge-common`（通用库）、`sebu-edge-data-manager`（本地存储）、`sebu-edge-formula`（公式引擎）、`sebu-edge-initializer`。
- 数据流：Producer 定时克隆 Modbus 请求模板 → Consumer 读寄存器 → `formula` 引擎(`rust_decimal` 高精度，支持 `@/$` 变量、`mergeRegister`、`getBits`、`bitScale`)换算 → `Vec<TsData>{timestamp, sn, key, value}` → `send_by_service(Service::MANAGER, &CONFIG.data_manager.url.realtime_data_batch)` **HTTP POST** 给 data-manager。
- data-manager：写本地 SQLite `ts_data`（按天 `tsdb_YYYYMMDD.db`）+ 无锁实时缓存(DashMap/scc)，断网兜底；提供 `/latest_data`、`/realtime_data_batch`、`/sync_sql` 等 HTTP API。
- **🔶 MQTT 未落地**：`properties.toml` 定义了完整 topic scheme（`we/meter/up/{iot}`、`we/config`、`we/control`）和 `[link.mqtt.publish/resume/monitor]` 段，`common/structs/mqtt.rs` 定义了 `Message`/`ControlParam` 结构体；但全仓库 **无 mqtt client 依赖**（无 rumqttc/paho），**无 `publish()`/`connect()` 调用**，配置段**无代码读取**，`emqx:1883` 仅出现在 .toml 与 `properties/mod.rs` 默认值。即 publisher 未在本 checkout 落地（可能在独立私有 crate 或 data-manager 其他分支）。

---

## 四、设备接入 / 转发层（🔶 源码未在本 checkout）

nacos 配置（`nacos/nacos_config_export_*/DEFAULT_GROUP/`）泄漏了以下服务，但 `project/` 与 `baobi/` 下**均无其源码**：
- `secp-iot-mqtt-auth`（EMQX 认证插件，证实 MQTT 接入是真实架构）
- `secp-iot-cec102-manager` / `secp-iot-cec102-process`（IEC104/国标 CEC102 处理）
- `secp-message-convert`（消息转换）
- `middle-forward` / `middle-edge-gw` / `middle-baobi-forwarding`（转发）
- 以及 nacos 导出中还能看到 `secp-iot-cec102-gate`、`middle-datacenter-service`、`middle-route-gw`、`middle-simulate-data`、`secp-iot-mqtt-auth.yaml` 等更多 dataId——这是"这些服务确凿存在"的**铁证**。

> **判断**：这不是项目没做 MQTT 接入/消息转换，而是你本地 checkout 只是 goodwe 仓库的子集，这几个模块（`secp-iot-*`、`message-convert`、`middle-*`）的源码未 clone 到本地（无 `.gitmodules`、`git submodule status` 为空，属独立模块未拉）。所以只能确认"它们存在于架构、且 Kafka 数仓 topic 是它们生产的"，讲不了其内部实现。

**推断**：它们是把设备数据（MQTT `we/meter/up` 或经 data-manager）送入 Kafka `secp-rich-*-event-data` 数仓 topic 的"第一跳"。面试讲到这里要诚实："接入转发层我在这次审计的 checkout 里没有源码，只确认它在架构里存在、且 Kafka 数仓 topic 是它生产的"——不要拍胸口讲它的内部实现。

---

## 五、Kafka 总线（Spring Kafka，无 Flink）+ prophet 数仓消费

- **Flink 不存在**：全仓库零 `org.apache.flink`。`secp-manager` 里 `biz/message/flink/` 包名有误导性，但 `AbstractKafkaDataSource` 用的是 `ConcurrentKafkaListenerContainerFactory`（**Spring Kafka**），不是 Flink 流处理。简历"Kafka→Flink→落库"的 Flink 是误称。
- **Kafka 真实存在且大量使用**：`secp-manager`、`secp-algorithm`、`secp-prophet`、`secp-watchman`、`secp-sniper`、`secp-settlement` 等都在 nacos 配了 Kafka。
- **prophet = 数仓真实落点**：
  - `DWStatusMessageDataSource`（`pcg-secp-prophet/.../dwh/status/`）注释明写"**数仓 status message 数据源**"，`@KafkaListener` 消费 `spring.kafka.data-warehouse-kafka.status-consumer.consume-topic`（topic 形如 `secp-rich-*-event-data`），转 `UnifiedMessage` 后写 `MessageRecordRepository`（即 prophet 的 ClickHouse）。
  - 同类还有 `DWEventDataSource`、`DWRunningStatusMessageDataSource`、`DWStaticStatusMessageDataSource` 等，覆盖设备事件/状态/运行/静态全量进数仓。
  - `MockController` 还向 `secp-device-realtime-latest-local` 发设备实时最新数据（key=设备 sn）。

---

## 六、watchman + algorithm：PG/TimescaleDB 聚合曲线（不是原始数仓）

- `secp-watchman` 消费告警/状态/通知类 topic（`prophet-device-communicate-status-change`、`prophet-alarm`、`prophet-notify`），并提供 5min 曲线查询 API。
- `secp-algorithm` 的 `StationMetricScheduleServiceImpl.generateRt5MinCurves()` 是 `@Async` 定时调度：遍历电站白名单 → `fetchStationMetricInfo()` 调 `watchmanService.getStationMetricData(stationId, curve5minReq)` 拉曲线 → `stationPvPower5minT0Repository.saveBatch(pvList)` 写 `_t0` 表（用 Redis 记上次执行时间做迟到数据收敛，T+1 同理写 `_t1`）。
- **关键定位**：PG/TimescaleDB 的 `_t0/_t1` 是**从 watchman 拉来的聚合曲线产物**，不是设备原始遥测直写。原始遥测进的是 prophet/ClickHouse 数仓。

---

## 七、存储分工与表结构（核心选型故事）

### 7.1 存储分工（四档标注）

| 数据 | 存储 | 查询模式 | 核实状态 |
|------|------|---------|---------|
| **设备事件/状态/运行记录（"数仓"）** | **ClickHouse**（secp-prophet，消费 Kafka dwh topic） | 海量消息记录的多维分析、跨租户跨周期 GROUP BY | ✅ 代码已坐实 |
| 聚合曲线（功率/发电量/环测，T+0/T+1） | **PG + TimescaleDB** | 按电站+时间范围，等值+范围查询 | ✅ 代码已坐实（算法服务从 watchman 拉写） |
| 告警事件检索 | **Elasticsearch** | 全文检索、多条件过滤 | ⚠️ 简历描述 |
| 业务消息（设备-产品关系/用户/操作日志） | **Spring Kafka 消费 → 各业务库** | 见 secp-manager/prophet 的 @KafkaListener | ✅ 代码已坐实 |
| 结算/账单/对账 OLAP | ClickHouse（简历称 RocketMQ 异步写入） | 跨租户跨周期 GROUP BY | ❌ 结算模块无 ClickHouse，未核实 |

### 7.2 TimescaleDB 真实 DDL（《Java基础面试》6.6 节）

```sql
CREATE TABLE algorithm.station_pv_power_5min_t0 (
    date_time  timestamp(6) NOT NULL,
    station_id bigint       NOT NULL,
    p          decimal      DEFAULT NULL,
    CONSTRAINT station_pv_power_5min_t0_pk PRIMARY KEY ("station_id", "date_time")
);
-- 转超表：按天 chunk
SELECT create_hypertable('algorithm.station_pv_power_5min_t0',
                         'date_time', chunk_time_interval => INTERVAL '1 day');
```

**hypertable 三层收益**：①写入永远落最新 chunk 热索引，吞吐稳定；②分区裁剪（查 7 天只扫 7 个 chunk）；③过期数据 DROP 整个 chunk 秒级无碎片。主键 `(station_id, date_time)` 既命中查询又天然写入幂等。

### 7.3 选型对比（高频追问）

**Q: 为什么时序存储选 PG + TimescaleDB，而不是 ClickHouse / InfluxDB？**
> "三点：①查询模式匹配——曲线查询是'按电站+时间范围'，关系模型直接命中，对业务库透明（还是一张表、标准 SQL）；②生态统一——业务库本就是 PG，备份/监控/运维栈全复用；③事务一致——时序表和业务表同库，补录修正可走事务，ClickHouse 做不到。代价是极端聚合不如 ClickHouse，所以重 OLAP 的设备事件分析单独走 prophet/ClickHouse 数仓——各干各的。"

**Q: ClickHouse 在链路里干什么？**
> "ClickHouse 是**真正的'数仓'**，在告警消息中心 secp-prophet：消费 Kafka 的数仓 topic（`secp-rich-*-event-data`），把设备事件/状态/运行记录落 ClickHouse 做跨租户、跨周期的多维分析（某租户本月各 eventCode 触发次数、持续时长分布），这是列存+向量化主场。它和 PG/TimescaleDB 的聚合曲线是两条独立链路，不是同一份数据双写。"

**Q: 那为什么不用 hypertable 替代分库分表？**
> "正是不分库分表的原因。传统 MySQL 按 station_id 哈希分库+按时间分表（ShardingSphere），路由和跨分片查询都复杂；PG+TimescaleDB 用数据库原生分区替代，海量写入下保持单库简洁。"

---

## 八、数据质量兜底（关联 secp-data-config）

- **稽核任务**：按调度周期检测异常（断点、越界、缺失率）
- **数据补录**：AVG/COPY/STANDARD 策略（策略模式），补录走 PG 事务写时序表
- **边缘网关断线告警**：连续失败 6 次触发飞书富文本告警，Redis 存告警状态
- **MQTT 认证管理**：设备接入凭证发放与吊销（secp-iot-mqtt-auth），防止脏数据源头

---

## 九、高频追问 Q&A

**Q: 千万级设备怎么保证不丢数据？**
> 分段式：①边缘本地 SQLite 缓冲 + 实时缓存，断网不丢（iot 已坐实）；②数据经接入层进 Kafka，Kafka 多副本；③写入侧主键 `(station_id, date_time)` + UPSERT 幂等，重试安全；④算法服务用 Redis 记上次执行时间，迟到数据 T+1 重算收敛。简历提的 Flink EXACTLY_ONCE 未核实，讲"主键幂等 + 调度收敛"更稳。

**Q: 数据重复了怎么办？**
> 三层兜底：Kafka 至少一次 → 算法服务落库前按 `latestData` 时间剔除已存在点（代码 `removeIf(dat.getDateTime() <= latestData)`）→ 主键约束物理挡重 → T+1 跑批最终收敛。

**Q: 几十亿行时序表查询怎么不慢？**
> hypertable 按天 chunk 分区裁剪 + 主键 `(station_id, date_time)` 索引；曲线页查 `_t0`，聚合页走 `_t1` 预聚合。

**Q: 设备量大了一亿条/天还能撑吗？**
> 扩容路径：Kafka 加分区（前期预留）→ 算法服务水平扩展实例 + PG 攒批写入 + 连接池调优；PG 侧 chunk 分布式友好，可上 TimescaleDB 多节点版/读副本。ClickHouse 数仓侧按设备 sn 分片。瓶颈先出现在 PG 攒批写入。

**Q: 维度信息变了历史数据怎么办？**
> 时序表只存 `station_id`+数值，维度在业务库；查询时 biz-data 从 Redis/PG 拿维度关联（记"当前归属"）。需要"历史归属"的分析走 prophet/ClickHouse（落库时已快照）。这是和传统数仓缓慢变化维的差异点。

**Q: 为什么不用 Flink？**
> 坦诚：实时聚合用 Spring Kafka 消费 + `@Async` 定时调度（`secp-algorithm` 的 `generateRt5MinCurves`）实现，没引入 Flink 集群——避免额外组件和运维。若被追问更重的流计算需求，可说"聚合粒度是 5min 定时就够，没必要 Flink 的毫秒级 Exactly-Once"。

---

## 十、代码审计结论（2026-08-27，基于 G:\Project\goodwe 核实）

| 简历/原稿说法 | 代码核实结果 | 结论 |
|------|------|------|
| 设备时序存 PG + TimescaleDB（T+0/T+1） | ✅ DDL 有 `station_pv_power_5min_t0/t1` 等全部 `create_hypertable`；`StationPvPower5minT0RepositoryImpl`（MyBatis-Plus）在 `pcg-secp-algorithm`；写入由 `StationMetricScheduleServiceImpl.generateRt5MinCurves`(@Async) 从 watchman 拉写 | **已坐实（但属聚合曲线产物，非原始数仓）** |
| MQTT 入站按 topic 路由 handler | ✅ `secp-manager/.../MqttConfiguration.java`：`MqttPahoMessageDrivenChannelAdapter` 入站，`@ServiceActivator` 按 topic 交 `MessageHandlerFactory` 分发；但**仅 `operationlog/frontend` 包**（操作日志/前端消息），全仓库无服务订阅 `we/meter/up` 设备遥测 topic | **已坐实（范围是操作日志/前端，非设备遥测）** |
| ClickHouse = 数仓 | ✅ `secp-prophet` 消费 Kafka `secp-rich-*-event-data`（data-warehouse-kafka）→ `MessageRecordRepository` → ClickHouse；`DWStatusMessageDataSource` 注释"数仓 status message 数据源" | **已坐实（数仓=prophet/ClickHouse，非 PG，也非结算）** |
| ClickHouse 存结算 OLAP（RocketMQ 异步写入） | 🔶 结算模块（`pcg-secp-electricity-settlement-payment`）源码内无 ClickHouse 依赖；但 nacos 有 `middle-datacenter-service`/`middle-baobi-forwarding` 等独立数据服务 dataId，结算/对账真实落点可能在未含模块 | **源码未在本 checkout，无法逐行核实；架构存在，不否定** |
| Kafka → Flink → 落库 全链路 | ❌ 全仓库零 `org.apache.flink`；真实是 Spring Kafka（`AbstractKafkaDataSource`）+ 定时调度；`flink` 仅包名误导 | **Flink 不存在，已纠正为 Spring Kafka + 调度** |
| 边缘采集器直接 MQTT publish 到 EMQX | ❌ iot（Rust）无 mqtt client 依赖，无 `publish()`/`connect()`；`[link.mqtt.*]` 配置段无代码读取；`task_manager.rs:362` 出门走 HTTP POST data-manager | **出门实为 HTTP，MQTT 待落地** |
| 设备接入/MQTT→Kafka 第一跳（secp-iot-*/message-convert/middle-forward） | 🔶 nacos 配置泄漏其存在，但 `project/`、`baobi/` 下无源码 | **源码未在本 checkout，仅确认存在于架构** |

> **🔶 的含义重申**：本文所有 🔶 标注 = "本地 checkout 不含该模块源码"，**不等于项目没做**（nacos 配置证明服务存在）。之前几轮我一度把缺失当成"简历夸大/未核实"是错的，在此撤回——正确态度是"架构可信，细节源码不在本地"。

**给面试的最安全口径**：
- **数仓** = prophet/ClickHouse，消费 Kafka dwh topic 存设备事件/状态/运行记录——confidently 讲，代码可佐证。
- **PG/TimescaleDB 的 `_t0/_t1`** = 算法服务从 watchman 拉的聚合曲线产物，不是原始遥测数仓——讲清这层关系加分。
- **Flink** = 不存在，坦诚讲 Spring Kafka + 定时调度。
- **边缘出门** = HTTP 到 data-manager，不要说"采集器直接 MQTT publish 到 EMQX"。
- **接入/转发层** = 承认源码未在本 checkout，只讲它在架构里存在、生产 Kafka 数仓 topic。
- **结算链路** = Redisson 分布式锁 + RocketMQ 事务消息的结算幂等与异步对账，不挂 ClickHouse。

---

## 附：与现有文档的对应关系

| 本文档章节 | 对应现有材料 |
|-----------|-------------|
| 三、边缘 iot（Rust） | `G:\Project\goodwe\iot` 实测：出门 HTTP、formula 引擎、data-manager 本地缓冲 |
| 四、接入层 | nacos 配置泄漏（源码未含） |
| 五、Kafka / prophet 数仓 | `pcg-secp-prophet/.../dwh/*` 实测；《面试题_简历驱动版》7.6 需同步改为"数仓=prophet/ClickHouse" |
| 六、watchman + algorithm | `StationMetricScheduleServiceImpl.java`、`WatchmanService` 实测 |
| 七、存储分工 / DDL | 《Java基础面试》6.6（TimescaleDB DDL，已核对）+ 代码库 prophet ClickHouse |
| 八、数据质量 | 《SECP平台_面试向业务逻辑摘要》secp-data-config 节 |
| 十、审计 | 基于 G:\Project\goodwe 实际代码（2026-08-27 多轮） |

> **待办**：《面试题_简历驱动版》7.6 节仍按"设备时序/发电量存 ClickHouse"+"Flink 落库"写，与本文矛盾（数仓=prophet/ClickHouse 设备事件；聚合曲线=PG/TimescaleDB；无 Flink）。建议同步修正 7.6 与 7.4 节，避免两份文档打架。
