# Java 基础面试题 — 固德威 SECP 业务场景结合版

> **本文档特色**：每个 Java 基础知识点均结合固德威 SECP 智慧能源云平台的**实际代码**和**业务场景**进行讲解，让面试回答有深度、有场景、有代码。
> 
> **知识来源**：pdai.tech Java 全栈知识体系 + 固德威 SECP 30+ 微服务实际代码（39000+ Java 文件）
> 
> **覆盖模块**：Java 集合框架 | Java 多线程与并发 | Java IO/NIO | JVM | 设计模式 | 数据库 | Redis | 消息队列 | Spring/Spring Boot/Spring Cloud | 微服务与分布式 | 计算机网络 | Netty与MQTT | Java 8+ 新特性 | 安全认证与OAuth2 | DevOps与容器化 | 数据结构（树）

---

## 目录

- [一、Java 集合框架](#一java-集合框架)
  - [1.1 HashMap 底层原理](#11-hashmap-底层原理)
  - [1.2 ConcurrentHashMap 线程安全实现](#12-concurrenthashmap-线程安全实现)
  - [1.3 ArrayList vs LinkedList](#13-arraylist-vs-linkedlist)
  - [1.4 CopyOnWriteArrayList 读写分离](#14-copyonwritearraylist-读写分离)
  - [1.5 集合在 SECP 中的实际应用](#15-集合在-secp-中的实际应用)
  - [1.6 TreeMap / TreeSet：排序的利器](#16-treemap--treeset排序的利器)
  - [1.7 LinkedHashMap：插入顺序与 LRU](#17-linkedhashmap插入顺序与-lru)
  - [1.8 HashSet / HashMap 去重的本质](#18-hashset--hashmap-去重的本质)
  - [1.9 Queue / BlockingQueue 家族](#19-queue--blockingqueue-家族生产者-消费者的骨架)
  - [1.10 fail-fast 与 ConcurrentModificationException](#110-fail-fast-与-concurrentmodificationexception)
- [二、Java 多线程与并发](#二java-多线程与并发)
  - [2.1 线程池 ThreadPoolExecutor 核心原理](#21-线程池-threadpoolexecutor-核心原理)
  - [2.2 CompletableFuture 异步编排](#22-completablefuture-异步编排)
  - [2.3 分布式锁 Redisson 实现原理](#23-分布式锁-redisson-实现原理)
  - [2.4 CAS 与原子类](#24-cas-与原子类)
  - [2.5 AQS 与 ReentrantLock](#25-aqs-与-reentrantlock)
  - [2.6 volatile 与 synchronized](#26-volatile-与-synchronized)
  - [2.7 ThreadLocal 与跨线程上下文传递](#27-threadlocal-与跨线程上下文传递)
- [三、Java IO/NIO](#三java-ionio)
  - [3.1 BIO / NIO / AIO 对比](#31-bio--nio--aio-对比)
  - [3.2 零拷贝技术](#32-零拷贝技术)
  - [3.3 IO 在 SECP 中的实际应用](#33-io-在-secp-中的实际应用)
- [四、JVM](#四jvm)
  - [4.1 JVM 内存结构](#41-jvm-内存结构)
  - [4.2 垃圾回收机制](#42-垃圾回收机制)
  - [4.3 类加载机制](#43-类加载机制)
  - [4.4 Java 内存模型 (JMM)](#44-java-内存模型-jmm)
  - [4.5 JVM 调优实战](#45-jvm-调优实战)
- [五、设计模式](#五设计模式)
  - [5.1 策略模式 (Strategy)](#51-策略模式-strategy)
  - [5.2 模板方法模式 (Template Method)](#52-模板方法模式-template-method)
  - [5.3 责任链模式 (Chain of Responsibility)](#53-责任链模式-chain-of-responsibility)
  - [5.4 代理模式 (Proxy)](#54-代理模式-proxy)
  - [5.5 外观模式 (Facade)](#55-外观模式-facade)
  - [5.6 观察者模式 (Observer)](#56-观察者模式-observer)
  - [5.7 单例模式 (Singleton)](#57-单例模式-singleton)
  - [5.8 工厂模式 (Factory)](#58-工厂模式-factory)
  - [5.9 建造者模式 (Builder)](#59-建造者模式-builder)
  - [5.10 适配器模式 (Adapter)](#510-适配器模式-adapter)
  - [5.11 设计模式在 SECP 中的综合应用](#511-设计模式在-secp-中的综合应用)
- [六、数据库（PostgreSQL / MySQL）](#六数据库postgresql--mysql)
  - [6.1 索引原理（B+树）与最左前缀](#61-索引原理b树与最左前缀)
  - [6.2 事务与隔离级别](#62-事务与隔离级别)
  - [6.3 连接池 HikariCP](#63-连接池-hikaricp)
  - [6.4 MyBatis / MyBatis-Plus](#64-mybatis--mybatis-plus)
  - [6.5 多数据源与读写分离](#65-多数据源与读写分离)
  - [6.6 时序数据与 TimescaleDB](#66-时序数据与-timescaledb)
  - [6.7 慢 SQL 排查与优化](#67-慢-sql-排查与优化)
  - [6.8 MySQL vs PostgreSQL](#68-mysql-vs-postgresql)
- [七、Redis](#七redis)
  - [7.1 Redis 数据结构与底层实现](#71-redis-数据结构与底层实现)
  - [7.2 缓存三剑客（穿透 / 击穿 / 雪崩）](#72-缓存三剑客穿透--击穿--雪崩)
  - [7.3 缓存与数据库一致性](#73-缓存与数据库一致性)
  - [7.4 分布式锁 Redisson](#74-分布式锁-redisson)
  - [7.5 Redis Pub/Sub 轻量级消息](#75-redis-pubsub-轻量级消息)
  - [7.6 持久化与高可用](#76-持久化与高可用)
- [八、消息队列（Kafka / RocketMQ）](#八消息队列kafka--rocketmq)
  - [8.1 为什么用 MQ？三大作用与引入风险](#81-为什么用-mq三大作用与引入风险)
  - [8.2 Kafka 核心原理](#82-kafka-核心原理)
  - [8.3 RocketMQ 核心原理](#83-rocketmq-核心原理)
  - [8.4 消息可靠性：不丢 / 不重 / 顺序](#84-消息可靠性不丢--不重--顺序)
  - [8.5 MQ 在 SECP 中的实际应用](#85-mq-在-secp-中的实际应用)
- [九、Spring / Spring Boot / Spring Cloud](#九spring--spring-boot--spring-cloud)
  - [9.1 IOC 与 Bean 生命周期](#91-ioc-与-bean-生命周期)
  - [9.2 循环依赖与三级缓存](#92-循环依赖与三级缓存)
  - [9.3 AOP：JDK 动态代理 vs CGLIB](#93-aopjdk-动态代理-vs-cglib)
  - [9.4 Spring 事务：传播与失效](#94-spring-事务传播与失效)
  - [9.5 Spring Boot 自动装配原理](#95-spring-boot-自动装配原理)
  - [9.6 Spring Cloud 组件在 SECP 的落地](#96-spring-cloud-组件在-secp-的落地)
  - [9.7 @Async 异步与 MDC 上下文传递](#97-async-异步与-mdc-上下文传递)
  - [9.8 Spring 高频问答速答](#98-spring-高频问答速答)
- [十、微服务与分布式](#十微服务与分布式)
  - [10.1 服务拆分：SECP 的域划分](#101-服务拆分secp-的域划分)
  - [10.2 服务间通信选型](#102-服务间通信选型)
  - [10.3 分布式事务：SECP 为什么不用 Seata](#103-分布式事务secp-为什么不用-seata)
  - [10.4 链路追踪：自研轻量方案](#104-链路追踪自研轻量方案)
  - [10.5 幂等设计汇总](#105-幂等设计汇总)
  - [10.6 分布式 ID](#106-分布式-id)
  - [10.7 高频问答](#107-高频问答)
- [十一、计算机网络](#十一计算机网络)
  - [11.1 TCP 三次握手 / 四次挥手](#111-tcp-三次握手--四次挥手)
  - [11.2 TCP vs UDP](#112-tcp-vs-udp)
  - [11.3 TCP 粘包/拆包](#113-tcp-粘包拆包)
  - [11.4 HTTPS / TLS 握手](#114-https--tls-握手)
  - [11.5 HTTP 版本演进与 REST](#115-http-版本演进与-rest)
  - [11.6 从输入 URL 到页面展示](#116-从输入-url-到页面展示速答骨架)
- [十二、Netty 与 MQTT（设备接入）](#十二netty-与-mqtt设备接入)
  - [12.1 Netty 是什么 & Reactor 模型](#121-netty-是什么--reactor-模型)
  - [12.2 Netty 核心组件速答](#122-netty-核心组件速答)
  - [12.3 MQTT 协议核心](#123-mqtt-协议核心)
  - [12.4 SECP 中的 MQTT 代码](#124-secp-中的-mqtt-代码)
  - [12.5 高频问答](#125-高频问答)
- [十三、Java 8+ 新特性](#十三java-8-新特性)
  - [13.1 Lambda 表达式与函数式接口](#131-lambda-表达式与函数式接口)
  - [13.2 Stream API](#132-stream-api)
  - [13.3 Optional 与空指针防御](#133-optional-与空指针防御)
  - [13.4 方法引用](#134-方法引用)
  - [13.5 接口默认方法与静态方法](#135-接口默认方法与静态方法)
  - [13.6 新时间日期 API (java.time)](#136-新时间日期-api-javatime)
  - [13.7 其他新特性速答](#137-其他新特性速答)
- [十四、安全认证与 OAuth2](#十四安全认证与-oauth2)
  - [14.1 认证 vs 授权](#141-认证-vs-授权)
  - [14.2 OAuth2 四种授权模式](#142-oauth2-四种授权模式)
  - [14.3 JWT 原理](#143-jwt-原理)
  - [14.4 Token + Redis 会话管理](#144-token--redis-会话管理)
  - [14.5 Feign 令牌传递](#145-feign-令牌传递)
  - [14.6 SECP 安全架构全景](#146-secp-安全架构全景)
  - [14.7 高频问答](#147-高频问答)
- [十五、DevOps 与容器化](#十五devops-与容器化)
  - [15.1 Docker 基础](#151-docker-基础)
  - [15.2 容器化 JVM 调优](#152-容器化-jvm-调优)
  - [15.3 SkyWalking 链路追踪](#153-skywalking-链路追踪)
  - [15.4 jemalloc 内存分配器](#154-jemalloc-内存分配器)
  - [15.5 高频问答](#155-高频问答)
- [十六、数据结构·树（结合 SECP 拓扑树 / 组织树 / 权限树）](#十六数据结构树结合-secp-拓扑树--组织树--权限树)
  - [16.1 二叉树基础：遍历与性质](#161-二叉树基础遍历与性质)
  - [16.2 二叉搜索树 BST](#162-二叉搜索树-bst)
  - [16.3 AVL 树 vs 红黑树](#163-avl-树-vs-红黑树)
  - [16.4 B 树 / B+ 树（数据库索引核心）](#164-b-树--b-树数据库索引核心)
  - [16.5 堆（Heap）与优先队列](#165-堆heap与优先队列)
  - [16.6 字典树 Trie](#166-字典树-trie)
  - [16.7 N 叉树：SECP 中的实际应用](#167-n-叉树secp-中的实际应用)
  - [16.8 树的遍历：BFS vs DFS 在 SECP 中的对比](#168-树的遍历bfs-vs-dfs-在-secp-中的对比)
  - [16.9 线段树与树状数组速答](#169-线段树与树状数组速答)
  - [16.10 高频问答](#1610-高频问答)
- [十七、综合面试场景题](#十七综合面试场景题)

---

## 一、Java 集合框架

### 1.1 HashMap 底层原理

**pdai 知识点回顾**：

> HashMap 是面试频率最高的集合类，核心要点：JDK 8 的底层结构是**数组 + 链表 + 红黑树**。

**面试标准回答**：

HashMap 在 JDK 8 中的数据结构是 `Node<K,V>[]` 数组 + 链表/红黑树。每个数组位置称为一个"桶"(bucket)，初始容量 16，负载因子 0.75。

**核心机制**：

| 机制          | 说明                                              |
| ----------- | ----------------------------------------------- |
| **hash 计算** | `(h = key.hashCode()) ^ (h >>> 16)`，高位低位异或，减少碰撞 |
| **定位桶**     | `(n-1) & hash`，n 是数组长度（2 的幂），等价于取模但更快           |
| **链表转红黑树**  | 链表长度 ≥ 8 且数组长度 ≥ 64 时转换；红黑树节点 ≤ 6 时退化为链表        |
| **扩容**      | 元素数量 > 容量 × 负载因子(0.75) 时，容量翻倍，重新 hash           |
| **线程不安全**   | 多线程 put 可能导致数据丢失；JDK 7 头插法会导致环形链表死循环            |

**结合 SECP 业务场景**：

在 SECP 电费结算模块中，`BizFileServiceImpl` 生成结算单文件时大量使用 HashMap 来组织模板数据：

```java
// BizFileServiceImpl.java — 结算单数据组装
Map<String, Object> map = new HashMap<>();
map.put("customName", settlement.getCustomName());
map.put("stationName", settlement.getStationName());
map.put("investorTenantName", settlement.getSettlementCompanyName());
// ...大量 key-value 数据填充到 HashMap 中，传递给模板引擎渲染
```

**面试加分回答**：

> "在我们 SECP 平台的电费结算模块中，结算单文件生成时需要将电站信息、电表读数、峰平谷电价等几十个字段填充到 HashMap 中传给 Thymeleaf 模板引擎渲染。这里选 HashMap 而不是 LinkedHashMap，因为模板渲染不需要保证 key 的插入顺序，HashMap 的 O(1) 查找性能更好。但需要注意 HashMap 是线程不安全的——如果多个线程同时往一个 HashMap 写数据可能导致数据丢失，所以在并发的批量审批场景中，我们用的是 `CompletableFuture` + 独立线程池，每个任务内部操作各自的局部变量，不共享 HashMap 实例。"

---

### 1.2 ConcurrentHashMap 线程安全实现

**pdai 知识点回顾**：

> ConcurrentHashMap 是线程安全的 HashMap，JDK 7 用分段锁(Segment)，JDK 8 改为 CAS + synchronized。

**JDK 8 核心实现**：

```
ConcurrentHashMap 结构：Node[] table
  ├── 桶为空        → CAS 写入（无锁）
  ├── 桶非空(链表)  → synchronized 锁住头节点
  └── 桶是红黑树   → synchronized 锁住 TreeBin
```

**关键设计**：

- **CAS 优化**：空桶直接用 CAS 写入，不需要加锁，读操作完全无锁
- **锁粒度细化**：从 JDK 7 的 Segment(默认 16 段) → JDK 8 的桶级锁，并发度大幅提升
- **sizeCtl 机制**：用 volatile + CAS 控制扩容，多线程协助扩容(transfer)

**结合 SECP 业务场景**：

SECP 平台中，`UserThingPermissionCache` 使用 ConcurrentHashMap 缓存用户物权限数据：

```java
// UserThingPermissionCache.java — 用户物权限缓存
public class UserThingPermissionCache implements PermissionCache {
    // 多线程环境下的权限缓存，查询请求并发访问
    private final Map<Long, UserThingCache> cacheMap = new ConcurrentHashMap<>();

    @Override
    public void setCache(UserThingCache cache) {
        cache.setTs(System.currentTimeMillis());
        cacheMap.put(cache.getUserId(), cache);  // 并发写
    }

    @Override
    public UserThingCache getCache(Long userId) {
        UserThingCache cache = cacheMap.get(userId);  // 并发读，无锁
        if (Objects.nonNull(cache)) {
            if (!cache.isExpired()) {
                return cache;  // 有效直接返回
            } else {
                cacheMap.remove(userId);  // 过期移除
            }
        }
        return null;
    }
}
```

另一个更复杂的场景是 `TagMismatchCollector`，使用了 `AtomicReference<ConcurrentHashMap>` 实现缓冲区切换：

```java
// TagMismatchCollector.java — 指标匹配失败收集器
private final AtomicReference<ConcurrentHashMap<String, MismatchRecord>> buffer =
        new AtomicReference<>(new ConcurrentHashMap<>());

public void collect(String tagCode, ...) {
    String key = tagCode + "|" + physicalLevel + "|" + timeliness + ...;
    // compute 是原子操作：不存在则创建，存在则计数+1
    buffer.get().compute(key, (k, existing) -> {
        if (existing == null) {
            return new MismatchRecord(tagCode, physicalLevel, timeliness, dimension, type);
        } else {
            existing.incrementCount();
            return existing;
        }
    });
}

public void syncToRedis() {
    // getAndSet：原子地替换整个 Map，旧 Map 安全遍历
    ConcurrentHashMap<String, MismatchRecord> oldBuffer = buffer.getAndSet(new ConcurrentHashMap<>());
    for (Map.Entry<String, MismatchRecord> entry : oldBuffer.entrySet()) {
        redisTemplate.opsForHash().increment(TAG_MISMATCH_KEY, entry.getKey(), entry.getValue().count);
    }
}
```

**面试加分回答**：

> "在 SECP 的统一指标查询中心(secp-biz-data)，我们用 `ConcurrentHashMap` 存储用户物权限缓存。由于平台服务全球 100+ 国家，查询请求是高度并发的，ConcurrentHashMap 的读操作完全无锁，写操作只锁单个桶，性能远优于 HashTable 的全表锁。在指标匹配异常收集中，我们更进一步用 `AtomicReference` 包装 `ConcurrentHashMap`，通过 `getAndSet` 原子操作实现缓冲区的无锁切换——收集线程往当前 buffer 写，定时任务原子地换出一个新 buffer 然后安全遍历旧 buffer 写入 Redis。这种设计借鉴了 `LinkedBlockingQueue` 的两把锁分离思想，读写不互斥。"

---

### 1.3 ArrayList vs LinkedList

**pdai 知识点回顾**：

| 特性   | ArrayList             | LinkedList   |
| ---- | --------------------- | ------------ |
| 底层   | 动态数组                  | 双向链表         |
| 随机访问 | O(1)                  | O(n)         |
| 头部插入 | O(n)                  | O(1)         |
| 扩容   | 1.5 倍扩容，Arrays.copyOf | 无需扩容         |
| 内存   | 连续内存，缓存友好             | 每个节点额外 32 字节 |

**ArrayList 扩容机制**：

```java
// JDK 源码
private void grow(int minCapacity) {
    int oldCapacity = elementData.length;
    int newCapacity = oldCapacity + (oldCapacity >> 1);  // 1.5 倍
    elementData = Arrays.copyOf(elementData, newCapacity);
}
```

**结合 SECP 业务场景**：

在 `DefaultTagDirectory` 中，静态指标列表使用 ArrayList 存储，因为指标在系统启动时一次性加载，后续只遍历读取：

```java
// DefaultTagDirectory.java
private static final List<MetricTagInfo> STATIC_TAG_LIST = new ArrayList<>();

static {
    STATIC_TAG_LIST.addAll(StaticEnumTagDirectoryInit.getMetricTagList());
}
// 后续操作：遍历分类
STATIC_TAG_LIST.forEach(this::classifyTagInfoDynamically);
```

在 `BizFileServiceImpl` 中，电表读数列表也使用 ArrayList，因为需要按索引访问：

```java
// BizFileServiceImpl.java
List<ThymeleafMeterReadingDTO> thymeleafMeterReadingDTOs = new ArrayList<>();
thymeleafMeterReadingDTOs.addAll(ceMeterReadings);
thymeleafMeterReadingDTOs.addAll(dischargeMeterReadings);
```

---

### 1.4 CopyOnWriteArrayList 读写分离

**pdai 知识点回顾**：

> CopyOnWrite 适合**读多写少**的场景。写时复制一份新数组，读时无锁。

**结合 SECP 业务场景**：

SECP 的告警事件配置、通知规则等元数据，在运行时偶尔更新但频繁读取，适合用 CopyOnWriteArrayList。在配置中心场景下，规则模板加载后只偶尔变更但高频被多线程读取。

**面试标准回答**：

> "CopyOnWriteArrayList 在写入时通过 `ReentrantLock` 加锁，复制出一个新数组写入，然后更新 volatile 引用指向新数组。读取时直接读 volatile 引用，完全无锁。它的缺点是写入需要复制整个数组，不适合频繁写入的场景。在 SECP 中，我们的告警通知规则配置就是读多写少的场景——配置后基本不变，但每次告警触发都需要读取规则匹配，这种场景下 CopyOnWriteArrayList 比加锁的 ArrayList 性能好很多。"

---

### 1.5 集合在 SECP 中的实际应用

**三层嵌套 ConcurrentHashMap 架构**：

```java
// DefaultTagDirectory.java — 指标目录的三层嵌套结构
// 物理层级 → 时间层级 → 指标编码 → 指标信息
private static final Map<String, Map<String, Map<String, MetricTagInfo>>> DYNAMIC_TAG_MAPS
    = new ConcurrentHashMap<>();

// 初始化：9 个物理层级 × 2 个时间层级 = 18 个并发 Map
private static void initializeDynamicMaps() {
    for (PhysicalLevelEnum physicalLevel : PhysicalLevelEnum.values()) {
        Map<String, Map<String, MetricTagInfo>> timeLevelMaps = new ConcurrentHashMap<>();
        for (TimelinessEnum timeLevel : TimelinessEnum.values()) {
            timeLevelMaps.put(timeLevel.getCode(), new ConcurrentHashMap<>());
        }
        DYNAMIC_TAG_MAPS.put(physicalLevel.getCode(), timeLevelMaps);
    }
}
```

**面试加分回答**：

> "在 SECP 的统一指标查询中心，我们设计了一个三层嵌套的 ConcurrentHashMap 结构来存储指标目录：第一层是物理层级(平台/租户/场站/系统/设备/节点等 9 级)，第二层是时效性(T+0 实时/T+1 历史)，第三层是指标编码到指标信息的映射。这种设计使得查询任意层级的指标时都能 O(1) 定位，且全部线程安全。系统初始化时按枚举预创建所有层级的 Map，避免运行时动态创建带来的并发问题。"

---

### 1.6 TreeMap / TreeSet：排序的利器

**pdai 知识点回顾**：TreeMap 基于红黑树实现，put/get/remove 均为 O(log n)，Key 有序（自然排序或 Comparator）。

**SECP 真实场景 1 — 第三方 API 签名（参数按 Key 排序拼接）**：

```java
// YongSignHelper.java — 永中电子签章 API 签名工具
if (params instanceof TreeMap) {
    treeMap = (TreeMap<String, String>) params;
} else {
    treeMap = new TreeMap<>(params);   // 强制转 TreeMap，按 key 字典序排列
}
// 之后按 treeMap 顺序拼接 key=value&... 再 MD5/RSA 签名
```

> 为什么签名必须用 TreeMap？因为几乎所有第三方开放平台（微信支付、阿里云、永中签章）的签名规则都是"参数按 ASCII 字典序排列后拼接"。TreeMap 天然有序，不需要额外 sort，直接遍历即满足签名规范。

**SECP 真实场景 2 — 时间区间自动排序合并**：

```java
// GenerateAnalysisServiceImpl.java — 发电分析，合并时间交集
Map<DateTime, DateTime> dateMap = new TreeMap<>();
Map<DateTime, DateTime> tempMap = new TreeMap<>();
// 注释原文：TreeMap能自动根据Key排序，只需要合并交集
```

> 分析报表时要把多个分段（并网时段、离网时段）按时间排序后合并交集，TreeMap 让"排序"这个步骤直接消失。

**SECP 真实场景 3 — TreeSet 去重排序**：

```java
// NodeUnifiedMetricServiceImpl.java
Set<LocalDate> sets = new TreeSet<>();          // 日期去重 + 升序
// AlertMessageServiceImpl.java
Set<String> occurredEventCodeSet = new TreeSet<>();  // 告警事件码去重排序
```

**面试标准回答**：

> "HashMap 是无序的 O(1) 查询，TreeMap 是红黑树 O(log n) 但 Key 有序。在 SECP 中有两类场景用 TreeMap：一是第三方 API 签名，签名规范要求参数按字典序排列，TreeMap 天然满足；二是数据分析中需要按时间 Key 排序合并区间。如果要'去重 + 排序'就用 TreeSet，比如告警事件码、日期集合的整理。LinkedHashMap 则用于保持插入顺序的场景。"

**红黑树高频追问速答**：

- 为什么不用 AVL 树？AVL 严格平衡，查询略快但插入删除旋转次数多；红黑树是近似平衡，增删性能更好，综合更优。
- JDK 1.8 HashMap 链表转红黑树的阈值是 8（且数组长度 ≥ 64），退化阈值是 6。为什么是 8？泊松分布下链表长度到 8 的概率约亿分之六，正常 hash 不会触发，是防御性设计。

---

### 1.7 LinkedHashMap：插入顺序与 LRU

**pdai 知识点回顾**：LinkedHashMap = HashMap + 双向链表。维护插入顺序（accessOrder=false）或访问顺序（accessOrder=true）。JDK 的 `Collections.synchronizedMap` 不保序，而 LinkedHashMap 保序——这是它存在的意义。

**SECP 真实场景 1 — 合同属性保持展示顺序**：

```java
// ContractServiceImpl.java — 合同属性按定义顺序输出到 PDF
Map<String, String> allAttributeCodeValues = new LinkedHashMap<>();
// UserFeedbackServiceImpl.java — 导出 Excel 列名映射，列顺序不能乱
Map<Integer, String> tempIndexColumnNameMap = new LinkedHashMap<>();
```

**SECP 真实场景 2 — Stream groupingBy 保持分组顺序**：

```java
// RelatedPartyServiceImpl.java
.collect(Collectors.groupingBy(RoleInfo::getTenantId, LinkedHashMap::new, Collectors.toList()));
```

> `Collectors.groupingBy` 默认返回 HashMap，分组结果顺序不可预期；传入 `LinkedHashMap::new` 后按租户首次出现的顺序返回——前端展示时列表顺序与请求顺序一致。

**LRU 手写（面试高频）**：

```java
public class LruCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;
    public LruCache(int capacity) {
        super(16, 0.75f, true);          // accessOrder = true：按访问顺序
        this.capacity = capacity;
    }
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > capacity;        // 超容量淘汰最久未访问的 entry
    }
}
```

> 面试追问"生产上直接用这个吗"——不，生产用 Caffeine（W-TinyLFU 算法，命中率比 LRU 高）或 Guava Cache，项目里 `TagMismatchCollector` 等组件就用了 Guava `CacheBuilder`。但手写 LRU 考察的是 LinkedHashMap 的 accessOrder 机制。

---

### 1.8 HashSet / HashMap 去重的本质

**pdai 知识点回顾**：HashSet 底层就是一个 HashMap（value 是固定的 PRESENT 对象）；对象去重依赖 hashCode + equals。

**SECP 场景 — 权限/去重判断**：项目中大量 `Set<Long> thingIds`、`Set<String> codes` 用于设备 ID、权限编码去重后批量查询（如 `UserThingPermissionCache`），核心是"O(1) 判断存在性"。

**高频追问**：

- 重写 equals 为什么必须重写 hashCode？——否则两个"相等"的对象 hash 到不同桶，Set/Map 中会出现重复。
- String 为什么适合做 Key？——不可变（hashCode 可缓存）、equals 实现规范。
- 对象作为 Key 后修改了会影响什么？——hashCode 变了，定位不到原桶，内存泄漏（WeakHashMap 可避免）。

---

### 1.9 Queue / BlockingQueue 家族：生产者-消费者的骨架

**pdai 知识点回顾**：

| 队列                    | 特性                                   | 典型用途                    |
| --------------------- | ------------------------------------ | ----------------------- |
| ArrayBlockingQueue    | 数组、有界、一把锁                            | 生产上限明确的场景               |
| LinkedBlockingQueue   | 链表、默认无界(Integer.MAX_VALUE)、两把锁(读写分离) | 吞吐优先                    |
| SynchronousQueue      | 不存元素、直接交接                            | CachedThreadPool / 直接提交 |
| PriorityBlockingQueue | 优先级堆                                 | 优先级任务                   |
| DelayQueue            | 延期获取                                 | 延迟任务、订单超时               |

**SECP 真实场景 1 — 电表功率补偿的内存队列（0）**：

```java
// ElectricMeterUpdateRatedPowerPlugin.java — sniper 服务
private final LinkedBlockingQueue<Long> processQueue = new LinkedBlockingQueue<>();
// 电表数据变更事件先进队列，异步消费，削峰 + 解耦
```

**SECP 真实场景 2 — 线程池的队列选型**：

```java
// middle-openapi-gw ThreadPoolConfig.java
new ThreadPoolExecutor(50, 200, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));
// MonitorCommonRpcServiceImpl.java — 监控 RPC 用 SynchronousQueue（不排队，满了直接扩线程）
new SynchronousQueue<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
// ExecutorMonitorTask.java — run-sentinel 服务巡检：读出队列积压量做监控告警
BlockingQueue<Runnable> queue = tp.getQueue();
```

**SECP 真实场景 3 — 分布式队列 Redisson RBlockingQueue（云台控制指令排队）**：

```java
// HikvisionFacadeServiceImpl.java — 海康摄像头 PTZ 云台控制
RBlockingQueue<PtzControlOrder> ptzControlWaitQueue =
    redissonClient.getBlockingQueue(String.format(PTZ_CONTROL_QUEUE_WAIT_KEY_FORMAT, deviceSerial));
// 同一摄像头一次只能执行一个云台动作，多 Pod 环境下用 Redis 队列做跨实例排队
```

**面试标准回答**：

> "BlockingQueue 是生产者-消费者模型的核心，put/take 在队列满/空时分别阻塞。SECP 里的用法分三层：单机削峰用 LinkedBlockingQueue（电表数据变更事件先入队异步处理）；线程池队列按场景选型——网关用 ArrayBlockingQueue(1000) 限制积压、监控 RPC 用 SynchronousQueue 让请求不排队直接扩线程；跨 Pod 的全局排队用 Redisson 的 RBlockingQueue，比如海康云台控制指令，同一设备串行执行。另外 run-sentinel 巡检服务会定期读线程池队列的 size 做积压告警。"

---

### 1.10 fail-fast 与 ConcurrentModificationException

**pdai 知识点回顾**：ArrayList 等集合的迭代器依赖 modCount，迭代期间结构性修改（add/remove）导致 modCount != expectedModCount，抛 ConcurrentModificationException。

**典型错误与正确姿势**：

```java
// 错误：foreach 中删除
for (Thing t : list) { if (...) list.remove(t); }  // CME!

// 正确 1：迭代器删除
Iterator<Thing> it = list.iterator();
while (it.hasNext()) { if (...) it.remove(); }

// 正确 2：JDK 8+ removeIf（底层就是迭代器）
list.removeIf(t -> t.getStatus() == DELETED);

// 正确 3：并发场景用 CopyOnWriteArrayList（迭代的是快照）
```

**SECP 实践**：批量删除设备/规则时统一用 `removeIf`；遍历权限缓存（ConcurrentHashMap）时即使并发修改也不抛异常——ConcurrentHashMap 的迭代器是弱一致性的（fail-safe），可能读到旧数据但不会崩。

**高频追问**：为什么 ArrayList 用 modCount 而不是加锁？——fail-fast 是"尽力检测"机制（best-effort），用异常快速暴露并发修改 bug，而不是保证线程安全；单线程下误改代码（迭代中 remove）也能被它拦住。

---

## 二、Java 多线程与并发

### 2.1 线程池 ThreadPoolExecutor 核心原理

**pdai 知识点回顾**：

> ThreadPoolExecutor 是 JUC 线程池的核心实现，七大参数 + 四大拒绝策略。

**七大核心参数**：

```java
public ThreadPoolExecutor(
    int corePoolSize,          // 核心线程数
    int maximumPoolSize,       // 最大线程数
    long keepAliveTime,        // 非核心线程空闲存活时间
    TimeUnit unit,             // 时间单位
    BlockingQueue<Runnable> workQueue,  // 任务队列
    ThreadFactory threadFactory,        // 线程工厂
    RejectedExecutionHandler handler     // 拒绝策略
)
```

**任务提交执行流程**：

```
提交任务
  │
  ├─ 当前线程数 < corePoolSize？ → 创建核心线程执行
  │
  ├─ 核心线程满？ → 放入任务队列
  │
  ├─ 队列满？ → 创建非核心线程(不超过 maximumPoolSize)
  │
  └─ 线程数 = maximumPoolSize 且队列满？ → 执行拒绝策略
```

**四大拒绝策略**：

| 策略                  | 行为                            |
| ------------------- | ----------------------------- |
| AbortPolicy(默认)     | 抛出 RejectedExecutionException |
| CallerRunsPolicy    | 由提交任务的线程执行                    |
| DiscardPolicy       | 静默丢弃                          |
| DiscardOldestPolicy | 丢弃队列最老的任务，重试提交                |

**结合 SECP 业务场景**：



SECP 电费结算模块配置了多个专用线程池，每个业务场景独立隔离：

```java
// ThreadPoolConfig.java — SECP 电费结算模块线程池配置
@Configuration
public class ThreadPoolConfig {

    @Bean("writeOssExecutor")           // 写 OSS 文件
    public ThreadPoolTaskExecutor writeOssExecutor() {
        return createThreadPool(10, 20, "write-oss-");
    }

    @Bean("getFileUrlExecutor")         // 获取文件 URL
    public ThreadPoolTaskExecutor getFileUrlExecutor() {
        return createThreadPool(10, 10, "get-file-url-");
    }

    @Bean("noticeSettlementExecutor")   // 结算通知
    public ThreadPoolTaskExecutor noticeSettlementExecutor() {
        return createThreadPool(10, 10, "notice-settlement-");
    }

    @Bean("syncYongYouExecutor")        // 用友开票同步
    public ThreadPoolTaskExecutor syncYongYouExecutor() {
        return createThreadPool(10, 10, "sync-yongYou-");
    }

    private ThreadPoolTaskExecutor createThreadPool(int coreSize, int maxSize, String name) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(coreSize);
        taskExecutor.setMaxPoolSize(maxSize);
        taskExecutor.setQueueCapacity(200);
        taskExecutor.setThreadNamePrefix(name);
        taskExecutor.setTaskDecorator(new MdcTaskDecorator());  // MDC 上下文传递
        // 队列满且达最大线程数时，由调用线程执行（不丢任务）
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
        return taskExecutor;
    }
}
```

**面试加分回答**：

> "在 SECP 电费结算模块中，我为不同业务场景配置了独立的线程池：写 OSS 文件、获取文件 URL、结算通知、用友开票同步等，每个线程池核心 10 线程最大 20，队列 200。关键设计有三点：
> 
> 1. **线程池隔离**：不同业务不共享线程池，避免某个业务(如 OSS 写入慢)拖垮其他业务。
> 2. **CallerRunsPolicy**：拒绝策略选 CallerRunsPolicy 而非默认的 AbortPolicy，因为结算文件生成不能丢任务——队列满时由调用线程自己执行，相当于背压限流，保证任务不丢失。
> 3. **MdcTaskDecorator**：通过 TaskDecorator 传递 MDC 日志上下文，保证异步线程中的日志能关联到原始请求链路，配合 SkyWalking 的 TraceId 实现全链路追踪。"

---

### 2.2 CompletableFuture 异步编排

**pdai 知识点回顾**：

> CompletableFuture 是 JDK 8 引入的异步编程工具，支持任务编排、组合、异常处理，比 Future 更强大。

**核心 API**：

| 方法                                 | 说明         |
| ---------------------------------- | ---------- |
| `supplyAsync(Supplier, Executor)`  | 异步执行有返回值任务 |
| `runAsync(Runnable, Executor)`     | 异步执行无返回值任务 |
| `thenApply / thenAccept / thenRun` | 前一步完成后继续处理 |
| `allOf(CF...)`                     | 等待所有完成     |
| `anyOf(CF...)`                     | 任一完成即返回    |
| `exceptionally / handle`           | 异常处理       |

**结合 SECP 业务场景**：

**场景一：结算单文件并行生成**

```java
// BizFileServiceImpl.java — 并行生成 PNG + PDF + Excel 三种格式结算单
public void generateBizFile(SettlementData settlementData, Map<String, Object> map,
                            Long bizId, BizTypeEnum bizTypeEnum) {

    // 三个文件生成任务并行提交到线程池
    CompletableFuture<FileOssInfoResp> pngFuture = CompletableFuture
        .supplyAsync(SupplierWrapper.of(() ->
            templateCenterAdapter.writeToOSS(TemplateTypeEnum.THYMELEAF, fileName,
                FileTypeEnum.PNG.getFileType(), map, thymeleafTemplateCode)),
            writeOssExecutor);  // 指定线程池

    CompletableFuture<FileOssInfoResp> pdfFuture = CompletableFuture
        .supplyAsync(SupplierWrapper.of(() ->
            templateCenterAdapter.writeToOSS(TemplateTypeEnum.THYMELEAF, fileName,
                FileTypeEnum.PDF.getFileType(), map, thymeleafTemplateCode)),
            writeOssExecutor);

    CompletableFuture<Optional<FileOssInfoResp>> excelFuture =
        CompletableFuture.supplyAsync(SupplierWrapper.of(() -> {
            try {
                return Optional.of(templateCenterAdapter.writeToOSS(
                    TemplateTypeEnum.JXLS, fileName,
                    FileTypeEnum.XLSX.getFileType(), map, jxlsTemplateCode));
            } catch (Exception e) {
                return Optional.empty();  // Excel 模板不存在不阻断主流程
            }
        }), writeOssExecutor);

    // 等待三个任务全部完成
    CompletableFuture.allOf(pngFuture, pdfFuture, excelFuture).join();

    // 获取结果组装 BizFile
    FileOssInfoResp pngOssInfoResp = pngFuture.join();
    FileOssInfoResp pdfOssInfoResp = pdfFuture.join();
    excelFuture.join().ifPresent(excelOssInfoResp -> {
        bizFile.setExcelName(...)
               .setExcelKey(excelOssInfoResp.getFileKey())
               .setExcelUrl(excelOssInfoResp.getFileUrl());
    });
}
```

**场景二：批量审批并行执行**

```java
// SettlementBatchApproveServiceImpl.java — 批量审批多线程
public ApprovalResultResp batchApprove(BatchIdRequest batchIdRequest) {
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);

    List<Long> unapprovedSettlementIds = getUnapprovedSettlementIds(batchIdRequest.getIdList());

    ThreadPoolTaskExecutor threadPoolTaskExecutor = createExecutorService();

    // 每个结算单审批作为一个独立任务提交
    CompletableFuture<Void> allTasks = CompletableFuture
        .allOf(unapprovedSettlementIds.stream()
            .map(id -> CompletableFuture.runAsync(RunnableWrapper.of(() -> {
                try {
                    settlementService.approve(tenantId, id);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("结算单审批失败,结算单id:{}", id, e);
                    failCount.incrementAndGet();
                }
            }), threadPoolTaskExecutor))
            .toArray(CompletableFuture[]::new));

    allTasks.get();  // 等待所有任务完成
    threadPoolTaskExecutor.shutdown();  // 临时线程池用完即关
}
```

**面试加分回答**：

> "在 SECP 电费结算模块中，我使用 CompletableFuture 实现了两个典型的异步编排场景：
> 
> **场景一**：结算单文件生成需要同时输出 PNG 图片、PDF 文件和 Excel 三种格式。如果串行执行需要 3 × 单文件生成时间，我用 `CompletableFuture.allOf()` 并行提交到 writeOssExecutor 线程池，总耗时降至 max(三个任务) 而非 sum。其中 Excel 生成可能因为租户未配置模板而失败，通过 `Optional.empty()` 优雅降级，不影响 PNG 和 PDF 的生成。
> 
> **场景二**：批量审批场景中，资方用户可能一次审批几十个结算单。我将每个审批任务包装为 `CompletableFuture.runAsync`，通过 stream + toArray 转为数组，用 `allOf().get()` 等待全部完成。关键细节是用 `RunnableWrapper.of()` (SkyWalking 提供) 包装 Runnable，保证 TraceId 传递到异步线程；用 `AtomicInteger` 统计成功/失败数，无需加锁。
> 
> **为什么不用 parallelStream？** 因为 parallelStream 使用的是 ForkJoinPool.commonPool()，所有应用共享，一个模块的任务会阻塞其他模块。独立线程池 + CompletableFuture 更可控。"

---

### 2.3 分布式锁 Redisson 实现原理

**pdai 知识点回顾**：

> Redisson 是 Redis 的 Java 客户端，提供分布式锁、限流、延迟队列等工具。其分布式锁基于 Redis + Lua 脚本实现。

**Redisson 分布式锁核心原理**：

```
加锁：Lua 脚本
  if redis.call('exists', key) == 0 then         -- 锁不存在
    redis.call('hset', key, threadId, 1)          -- Hash 结构存重入次数
    redis.call('pexpire', key, leaseTime)         -- 设置过期时间
    return nil
  end
  if redis.call('hexists', key, threadId) == 1 then -- 可重入
    redis.call('hincrby', key, threadId, 1)
    redis.call('pexpire', key, leaseTime)
    return nil
  end
  return redis.call('pttl', key)                   -- 返回剩余过期时间

解锁：Lua 脚本
  if redis.call('hexists', key, threadId) == 0 then
    return nil  -- 不是自己的锁
  end
  local count = redis.call('hincrby', key, threadId, -1)
  if count > 0 then
    redis.call('pexpire', key, nextTime)  -- 还有重入次数
    return 0
  else
    redis.call('del', key)                 -- 真正释放
    return 1
  end
```

**关键特性**：

- **可重入**：用 Hash 结构记录 threadId → 重入次数
- **看门狗(Watchdog)**：默认 30s 过期，每 10s 续期，防止业务未完成锁就过期
- **读写锁**：RReadWriteLock 支持读读不互斥、读写互斥、写写互斥

**结合 SECP 业务场景**：

```java
// BusinessLockAspect.java — SECP 电费结算分布式锁 AOP 切面
@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
public class BusinessLockAspect {

    private final RedissonClient redissonClient;

    @Pointcut("@annotation(com.goodwe.sebu.electricity.biz.service.lock.BusinessLock)")
    private void anyAddLockMethod() {}

    @Around("effectPointcut()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        // 构造锁 Key
        String lockEntityStr = String.valueOf(point.getArgs()[0]);
        Method targetMethod = ((MethodSignature) point.getSignature()).getMethod();
        BusinessLock businessLock = Optional.ofNullable(targetMethod.getAnnotation(BusinessLock.class))
            .orElseThrow(() -> new BusinessException(ErrorCodeEnum.SERVICE_ERROR_C0001));

        String lockKey = String.format(businessLock.lockFormat(), lockEntityStr);

        // 使用读写锁的写锁（独占）
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(lockKey);
        Lock lock = readWriteLock.writeLock();

        lock.lock();  // 加锁
        try {
            res = point.proceed(point.getArgs());  // 执行业务
        } catch (Exception e) {
            log.error("业务执行异常，锁对象: {}", lock, e);
            throw e;
        } finally {
            lock.unlock();  // 释放锁
        }
        return res;
    }
}
```

**面试加分回答**：

> "在 SECP 电费结算模块中，我设计了一个基于 AOP + Redisson 的声明式分布式锁框架。通过自定义 `@BusinessLock` 注解标记需要加锁的方法，AOP 切面自动拦截并加锁。
> 
> **为什么用 Redisson 而不是 Redis SETNX？**
> 
> 1. SETNX 不支持可重入，Redisson 用 Hash 结构记录 threadId + 重入次数，同一线程多次获取同一把锁不会死锁。
> 2. SETNX 需要手动设置过期时间，业务执行超过过期时间会导致锁被误释放。Redisson 有看门狗机制，默认每 10 秒检查并续期。
> 3. Redisson 提供了 `RReadWriteLock`，在结算场景中，查询结算单是读操作(共享锁)，审批结算单是写操作(独占锁)，读写不互斥提升并发性能。
> 
> **幂等性保障**：结算单生成流程是：获取分布式锁 → 检查是否已生成 → 生成结算单 → 释放锁。即使定时任务和手动触发同时执行，也不会重复生成。"

---

### 2.4 CAS 与原子类

**pdai 知识点回顾**：

> CAS(Compare-And-Swap) 是无锁编程的基础，三个操作数：内存值 V、预期值 A、新值 B。当 V==A 时更新为 B，否则重试。

**CAS 的三大问题**：

| 问题      | 说明                | 解决方案                        |
| ------- | ----------------- | --------------------------- |
| ABA 问题  | 值从 A→B→A，CAS 认为没变 | AtomicStampedReference(版本号) |
| 自旋开销    | 长时间不成功则 CPU 空转    | 限制自旋次数                      |
| 只保证一个变量 | 多个变量 CAS 无法保证     | AtomicReference 包装对象        |

**结合 SECP 业务场景**：

```java
// TagMismatchCollector.java — AtomicReference 实现无锁缓冲区切换
private final AtomicReference<ConcurrentHashMap<String, MismatchRecord>> buffer =
    new AtomicReference<>(new ConcurrentHashMap<>());

// 收集线程并发写入
public void collect(String tagCode, ...) {
    buffer.get().compute(key, (k, existing) -> { ... });
}

// 定时任务原子切换
public void syncToRedis() {
    // getAndSet = CAS 交换：原子地获取旧值并设置新值
    ConcurrentHashMap<String, MismatchRecord> oldBuffer =
        buffer.getAndSet(new ConcurrentHashMap<>());
    // 安全遍历旧 buffer 写入 Redis
    for (Map.Entry<String, MismatchRecord> entry : oldBuffer.entrySet()) {
        redisTemplate.opsForHash().increment(TAG_MISMATCH_KEY, entry.getKey(), entry.getValue().count);
    }
}

// SettlementBatchApproveServiceImpl.java — AtomicInteger 无锁计数
AtomicInteger successCount = new AtomicInteger(0);
AtomicInteger failCount = new AtomicInteger(0);
// 多线程并发累加，CAS 保证原子性
successCount.incrementAndGet();
failCount.incrementAndGet();
```

**面试加分回答**：

> "在 SECP 的指标匹配异常收集器中，我使用 `AtomicReference<ConcurrentHashMap>` 实现了无锁的缓冲区切换。收集线程高频往 buffer 写数据，定时任务每 5 分钟需要把 buffer 刷到 Redis。如果用 synchronized 或加锁，收集线程会被阻塞。用 `AtomicReference.getAndSet()` 原子地换出一个新空 buffer，收集线程立即写入新 buffer，定时任务安全遍历旧 buffer——全程无锁。
> 
> 在批量审批场景中，用 `AtomicInteger` 替代 `synchronized` 计数器，因为 AtomicInteger 底层是 CAS 自旋，在低竞争场景下性能优于加锁。"

---

### 2.5 AQS 与 ReentrantLock

**pdai 知识点回顾**：

> AQS(AbstractQueuedSynchronizer) 是 JUC 的基石，ReentrantLock、Semaphore、CountDownLatch 都基于它实现。核心是 FIFO 双向队列 + volatile int state。

**AQS 核心结构**：

```
AQS
  ├── volatile int state          // 同步状态（锁重入次数 / 信号量许可数 / 倒计数）
  ├── Node head                   // 等待队列头节点
  ├── Node tail                   // 等待队列尾节点
  └── Node { thread, prev, next, waitStatus }  // 等待节点

独占模式(ReentrantLock)：
  - acquire: CAS 修改 state，失败则入队 park
  - release: 修改 state，唤醒队首线程 unpark

共享模式(Semaphore/CountDownLatch)：
  - acquireShared: CAS 修改 state，失败入队
  - releaseShared: 修改 state，级联唤醒
```

**ReentrantLock vs synchronized**：

| 特性   | synchronized                      | ReentrantLock         |
| ---- | --------------------------------- | --------------------- |
| 实现   | JVM 内置(monitorenter/monitorenter) | AQS + CAS             |
| 公平锁  | 非公平                               | 支持公平/非公平              |
| 可中断  | 不可中断                              | `lockInterruptibly()` |
| 超时获取 | 不支持                               | `tryLock(timeout)`    |
| 条件变量 | 一个 wait/notify                    | 多个 Condition          |
| 自动释放 | 自动释放                              | 手动 unlock(finally)    |

**结合 SECP 业务场景**：

> SECP 中 Redisson 的 RLock 底层是 Redis + Lua 脚本实现的分布式锁，不是 AQS。但单机内的线程同步，如 CompletableFuture 批量审批中，`allOf().get()` 底层就是 AQS 的共享模式——每个任务完成时 `countDown`，state 减到 0 时唤醒等待的主线程。

---

### 2.6 volatile 与 synchronized

**pdai 知识点回顾**：

| 关键字          | 作用                 | 原理                              |
| ------------ | ------------------ | ------------------------------- |
| volatile     | 保证可见性 + 有序性(禁止重排序) | 内存屏障：写操作后刷新主存，读操作前从主存加载         |
| synchronized | 保证原子性 + 可见性 + 有序性  | monitorenter/monitorexit + 监视器锁 |

**volatile 应用场景**：

- DCL 单例的 instance 字段（防止指令重排导致拿到未初始化的对象）
- 状态标志位（一个线程写，多线程读）
- AQS 的 state 字段

**结合 SECP 业务场景**：

> 在 SECP 的 `TagMismatchCollector` 中，`AtomicReference` 内部的 `value` 字段就是 volatile 的，保证一个线程调用 `getAndSet()` 替换 buffer 后，其他收集线程立即可见新 buffer 的引用。如果不用 volatile，可能有线程还看到旧的 buffer 引用，导致数据写入到已经被切换掉的旧 buffer 中丢失。

---

### 2.7 ThreadLocal 与跨线程上下文传递

**pdai 知识点回顾**：

> ThreadLocal 为每个线程提供独立的变量副本，实现线程间数据隔离。底层是每个 Thread 对象的 `ThreadLocalMap`。

**ThreadLocal 内存模型**：

```
Thread
  └── ThreadLocalMap (每个 Thread 独有)
        ├── Entry (key = ThreadLocal 弱引用, value = 变量值)
        ├── Entry
        └── ...
```

**内存泄漏问题**：key 是弱引用，value 是强引用。ThreadLocal 被回收后 key=null 但 value 仍存在，导致 value 泄漏。解决方案：用完后 `remove()`。

**结合 SECP 业务场景**：

SECP 有一个专门的模块 `secp-context-carrier-agent`，通过 Java Agent + Javassist 字节码增强实现**零侵入的跨线程上下文传递**：

```
SECP 跨线程上下文传递架构：
  ┌─────────────────────────────────────────────────────────┐
  │  Java Agent (secp-context-carrier-agent)                │
  │  └── Javassist 字节码增强                                │
  │      └── 拦截 ThreadPoolExecutor.execute()               │
  │      └── 拦截 CompletableFuture.supplyAsync()            │
  │      └── 自动在提交 Runnable 时包装 TransmittableThreadLocal │
  └─────────────────────────────────────────────────────────┘
```

**面试加分回答**：

> "在 SECP 平台中，30+ 微服务之间存在大量的异步调用(CompletableFuture)和线程池任务。一个请求从进入网关开始就携带了租户 ID、用户 ID、TraceId 等上下文信息，这些信息存在 ThreadLocal 中。但线程池中的工作线程是复用的，默认不继承主线程的 ThreadLocal。
> 
> 我们通过 `secp-context-carrier-agent` 模块解决了这个问题——它是一个 Java Agent，在类加载时用 Javassist 字节码增强技术自动拦截 `ThreadPoolExecutor.execute()`、`CompletableFuture.supplyAsync()` 等方法，在任务提交时自动包装 `TransmittableThreadLocal`(阿里 TTL 框架)，实现跨线程的上下文传递，零代码侵入。
> 
> 同时配合 `MdcTaskDecorator` 在 ThreadPoolTaskExecutor 中传递日志 MDC 上下文，保证异步线程中的日志也带有 TraceId。"

---

## 三、Java IO/NIO

### 3.1 BIO / NIO / AIO 对比

**pdai 知识点回顾**：

| 模型  | 全称              | 特点                                    | 适用场景      |
| --- | --------------- | ------------------------------------- | --------- |
| BIO | Blocking IO     | 面向流，一个连接一个线程，读写阻塞                     | 连接数少且固定   |
| NIO | Non-blocking IO | 面向缓冲(Buffer)，多路复用(Selector)，一个线程管理多连接 | 连接数多但数据轻量 |
| AIO | Asynchronous IO | 异步非阻塞，操作系统回调通知                        | 连接数多且数据重  |

**BIO 核心类**：

```
InputStream → FileInputStream, ByteArrayInputStream, BufferedInputStream
OutputStream → FileOutputStream, ByteArrayOutputStream, BufferedOutputStream
Reader/Writer → FileReader/FileWriter, BufferedReader/BufferedWriter
```

**NIO 三大核心**：

```
Channel (通道)     → 双向读写 (FileChannel, SocketChannel, ServerSocketChannel)
Buffer (缓冲区)    → 读写容器 (ByteBuffer, CharBuffer, ...), flip()/clear()
Selector (选择器)  → 多路复用器，一个线程管理多个 Channel
```

**NIO 工作流程**：

```
1. ServerSocketChannel.open() → bind(port) → configureBlocking(false)
2. Selector.open()
3. channel.register(selector, SelectionKey.OP_ACCEPT)
4. while(true):
     selector.select()  // 阻塞直到有就绪事件
     Set<SelectionKey> keys = selector.selectedKeys()
     for key in keys:
       if key.isAcceptable() → accept 新连接
       if key.isReadable() → 读取数据
       if key.isWritable() → 写入数据
```

**结合 SECP 业务场景**：

> SECP 平台的设备通信层使用 MQTT + gRPC：
> 
> - **MQTT**：设备(逆变器、储能柜)上行数据采集，底层是 NIO + Netty 实现
> - **gRPC**：下行控制指令下发，基于 HTTP/2 多路复用
> 
> 设备数据采集流程：
> 
> ```
> 设备 → MQTT Broker → spring-integration-mqtt → Kafka → Flink → ES/ClickHouse
> ```
> 
> 这里的 MQTT 消息消费本质是 NIO 的 Reactor 模式——一个线程管理多个设备连接，通过 Selector 轮询就绪的通道读写数据，避免 BIO 模型下一个设备一个线程的资源浪费。平台对接全球 100+ 国家千万级设备，BIO 模型无法支撑。

---

### 3.2 零拷贝技术

**pdai 知识点回顾**：

> 零拷贝(Zero Copy)减少数据在内核空间和用户空间之间的拷贝次数。

**传统数据传输(4 次拷贝 + 4 次上下文切换)**：

```
磁盘 → 内核缓冲区 → 用户缓冲区 → Socket 缓冲区 → 网卡
  DMA拷贝      CPU拷贝       CPU拷贝       DMA拷贝
```

**mmap 优化(3 次拷贝 + 4 次切换)**：

```
磁盘 → 内核缓冲区(映射到用户空间) → Socket 缓冲区 → 网卡
  DMA拷贝      CPU拷贝           DMA拷贝
```

**sendfile 优化(2 次拷贝 + 2 次切换)**：

```
磁盘 → 内核缓冲区 → 网卡(通过 SG-DMA)
  DMA拷贝      DMA拷贝
```

**Java NIO 零拷贝实现**：0

- `FileChannel.transferTo()` → 底层调用 `sendfile()`
- `MappedByteBuffer` → 底层调用 `mmap()`

**结合 SECP 业务场景**：

> SECP 使用 Kafka 作为设备事件数据管道。Kafka 消费者从 Broker 拉取消息时，底层使用 `FileChannel.transferTo()` 实现零拷贝，将日志段文件直接传输到 Socket，不经过用户空间。这在大规模设备数据采集(千万级设备 × 秒级上报)场景下显著降低了 CPU 消耗。
> 
> 同时 RocketMQ 的消息存储也使用了 `MappedByteBuffer`(mmap)将 CommitLog 文件映射到内存，写入消息直接操作内存映射区域，减少一次 CPU 拷贝。

---

### 3.3 IO 在 SECP 中的实际应用

**文件下载与上传**：

```java
// BizFileServiceImpl.java — 结算单文件下载
// 本质是 BIO 流式写入 HTTP Response
public void getBizFile(Long bizId, BizTypeEnum bizTypeEnum, String fileType, HttpServletResponse resp) {
    BizFile bizFile = bizFileRepository.getByBizId(bizId, bizTypeEnum).orElseThrow(...);
    // 从 OSS 获取文件 URL，流式写入 HTTP Response
    FileUtils.downloadFileWriteToResponse(fileName, fileUrl, resp);
}
```

**SSE 流式推送**：

> SECP 的 `secp-statistics` 模块使用 SSE(Server-Sent Events) 流式推送曲线数据。SSE 本质是 HTTP 长连接 + chunked 传输，服务器持续向客户端 push 数据。底层是 Servlet 的异步 IO，一个线程可以处理多个 SSE 连连。

**面试加分回答**：

> "在 SECP 中，IO 模型贯穿了设备通信到数据展示全链路：
> 
> - **设备层**：MQTT(NIO/Netty Reactor) → 一个线程管理数千设备连接
> - **消息层**：Kafka/RocketMQ(零拷贝 sendfile/mmap) → 高吞吐设备事件管道
> - **存储层**：ES/ClickHouse → 列式存储 + 压缩，减少 IO
> - **展示层**：SSE 流式推送 → 避免轮询，减少 HTTP 连接数
> - **文件层**：OSS + HttpServletResponse 流式下载 → 结算单文件不落本地磁盘"

---

## 四、JVM

### 4.1 JVM 内存结构

**pdai 知识点回顾**：

> JVM 运行时数据区分为线程私有和线程共享两部分。

```
┌─────────────────────────────────────────────────────────────┐
│                     JVM 运行时数据区                          │
├──────────────┬──────────────┬───────────────────────────────┤
│  线程私有     │  线程私有     │  线程共享                      │
├──────────────┼──────────────┼───────────────┬───────────────┤
│ 程序计数器(PC)│ 虚拟机栈(栈帧)│  堆(Heap)     │  方法区/元空间  │
│ - 字节码行号   │ - 局部变量表  │ - 新生代       │ - 类信息       │
│ - 无 OOM      │ - 操作数栈    │   Eden:S0:S1  │ - 运行时常量池  │
│              │ - 动态链接    │   = 8:1:1     │ - 静态变量     │
│              │ - 返回地址    │ - 老年代       │ (JDK8前永久代)  │
├──────────────┼──────────────┼───────────────┴───────────────┤
│ 本地方法栈    │              │  堆外内存(Direct Memory)       │
│ - Native方法  │              │  - Netty ByteBuf              │
└──────────────┴──────────────┴───────────────────────────────┘
```

**关键参数**：

| 参数                      | 说明    | SECP 推荐值           |
| ----------------------- | ----- | ------------------ |
| -Xms                    | 堆初始大小 | 4G(与 Xmx 相同避免扩容抖动) |
| -Xmx                    | 堆最大大小 | 4G                 |
| -Xmn                    | 新生代大小 | 1.5G(堆的 3/8)       |
| -XX:MetaspaceSize       | 元空间初始 | 256m               |
| -XX:MaxMetaspaceSize    | 元空间最大 | 512m               |
| -Xss                    | 线程栈大小 | 512k(默认)           |
| -XX:MaxDirectMemorySize | 堆外内存  | 1G                 |

**结合 SECP 业务场景**：

> SECP 30+ 微服务容器化部署在 K8s 上，每个 Pod 通常分配 4C8G，JVM 配置 -Xms4g -Xmx4g -Xmn1536m。元空间设 -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m，因为 Spring Cloud + MyBatis Plus 大量使用 CGLIB 动态代理和反射，类加载较多，永久代(老版本)容易 OOM。
> 
> TLAB(Thread Local Allocation Buffer)：SECP 高并发设备数据采集场景，多线程频繁创建短生命周期对象(消息 DTO、事件对象)，TLAB 让每个线程在 Eden 区有独立分配缓冲区，避免分配竞争。
> 
> 逃逸分析：JIT 编译器分析对象作用域，未逃逸的对象做栈上分配 + 锁消除。SECP 中大量短生命周期 DTO 对象(如设备事件消息)经过逃逸分析后可在栈上分配，随方法结束自动释放，不占堆内存，减轻 GC 压力。

---

### 4.2 垃圾回收机制

**pdai 知识点回顾**：

**GC 算法**：

| 算法    | 说明                   | 适用  |
| ----- | -------------------- | --- |
| 标记-清除 | 标记存活 → 清除死亡，产生碎片     | 老年代 |
| 标记-复制 | 存活对象复制到另一半，清理原区域     | 新生代 |
| 标记-整理 | 标记存活 → 向一端移动 → 清理边界外 | 老年代 |

**分代回收**：

```
对象分配 → Eden
  │ Minor GC (复制算法)
  ├─ 存活 → Survivor (From/To 交替)
  │    │ 经历 15 次 Minor GC (-XX:MaxTenuringThreshold=15)
  │    └─ → 老年代
  │
  └─ 大对象 → 直接进老年代 (-XX:PretenureSizeThreshold)

老年代满 → Major GC/Full GC (标记-清除/标记-整理)
```

**GC Roots(可作为 GC 根节点)**：

1. 虚拟机栈中的引用对象(局部变量)
2. 方法区中静态变量引用的对象
3. 方法区中常量引用的对象
4. 本地方法栈中 JNI 引用的对象
5. Java 虚拟机内部引用(基本类型 Class 对象、常驻异常对象、类加载器)
6. 同步锁(synchronized)持有的对象

**垃圾回收器对比**：

| 回收器               | 特点            | 适用场景           |
| ----------------- | ------------- | -------------- |
| Serial            | 单线程，STW       | 客户端模式          |
| ParNew            | 多线程版 Serial   | 配合 CMS         |
| Parallel Scavenge | 吞吐量优先         | 后台计算           |
| CMS               | 低延迟，标记-清除     | JDK8 默认推荐(已废弃) |
| G1                | 分区+分代，可预测停顿   | JDK9+ 默认，大堆    |
| ZGC               | 染色指针，<10ms 停顿 | JDK11+ 大堆低延迟   |

**结合 SECP 业务场景**：

> SECP 平台推荐使用 **G1 回收器**(-XX:+UseG1GC)。原因：
> 
> 1. **大堆场景**：SECP 每个微服务 -Xmx4g，G1 适合 4G+ 堆内存，将堆划分为 2048 个 Region(默认 1-32M)，避免了 Full GC 的全局 STW。
> 2. **可预测停顿**：-XX:MaxGCPauseMillis=200，G1 会根据历史数据预测哪些 Region 回收价值最高(C-set)，在 200ms 内尽可能回收，保证接口可用性。SECP 场站概览页要求 200ms 响应，GC 停顿不能太长。
> 3. **SECP 设备数据管道的 GC 压力**：Kafka 消费 → Flink 处理 → ES 写入的链路中，每秒处理数千条设备事件消息，产生大量短生命周期对象。G1 的 Young GC 只回收新生代 Region，停顿时间可控。老年代回收(Mixed GC)逐步回收，不需要 Full GC。
> 
> **GC 调优经验**：
> 
> ```
> -XX:+UseG1GC
> -XX:MaxGCPauseMillis=200    # 目标停顿时间
> -XX:G1HeapRegionSize=16m    # Region 大小
> -XX:InitiatingHeapOccupancyPercent=45  # 老年代占用率触发 Mixed GC
> -XX:G1NewSizePercent=30     # 新生代最小占比
> -XX:G1MaxNewSizePercent=60  # 新生代最大占比
> ```

---

### 4.3 类加载机制

**pdai 知识点回顾**：

> 类加载过程：加载 → 验证 → 准备 → 解析 → 初始化 → 使用 → 卸载

**双亲委派模型**：

```
BootstrapClassLoader (rt.jar, java.lang.*)
  └── ExtClassLoader (ext/*.jar)
       └── AppClassLoader (classpath)
            └── CustomClassLoader (自定义)
```

**委派流程**：类加载请求先委托给父加载器，父加载器无法加载才自己加载。

**打破双亲委派的场景**：

1. SPI 机制(JDBC、SLF4J)：Thread.currentThread().getContextClassLoader()
2. Tomcat：每个 WebApp 有独立 ClassLoader
3. OSGi：模块化热部署

**结合 SECP 业务场景**：

> SECP 中有两个场景与类加载密切相关：
> 
> **1. secp-context-carrier-agent (Java Agent 字节码增强)**
> 
> 这是一个 Java Agent，在 JVM 启动时通过 `-javaagent:context-carrier-agent.jar` 加载。它利用 Instrumentation API 在类加载时通过 Javassist 修改字节码，拦截线程池和 CompletableFuture 的方法，自动注入上下文传递逻辑。这属于"字节码增强"技术，在类加载阶段织入横切逻辑，类似 AOP 但发生在更底层。
> 
> **2. Spring Boot Starter 自定义类加载**
> 
> SECP 自研了 `goodwe-persistence-spring-boot-starter` 和 `goodwe-rocketmq-spring-boot-starter`，这些 Starter 依赖 Spring Boot 的 SPI 机制(spring.factories / AutoConfiguration.imports)。Spring Boot 通过 `AutoConfigurationImportSelector` 加载自动配置类，底层使用 `Thread.currentThread().getContextClassLoader()` 打破双亲委派——因为 BootstrapClassLoader 加载不了用户 classpath 下的配置类。

---

### 4.4 Java 内存模型 (JMM)

**pdai 知识点回顾**：

> JMM(Java Memory Model) ≠ JVM 内存结构。JMM 是一组规范，定义多线程环境下共享变量的访问规则。

**核心概念**：

```
线程 A 工作内存          线程 B 工作内存
  (本地缓存副本)           (本地缓存副本)
      ↕ read/load          ↕ read/load
    ┌─────────────────────────────┐
    │       主内存 (共享变量)        │
    └─────────────────────────────┘
      ↕ write/store          ↕ write/store
```

**三大特性**：

| 特性  | 含义          | 保证手段                           |
| --- | ----------- | ------------------------------ |
| 原子性 | 操作不可分割      | synchronized, Lock, Atomic*    |
| 可见性 | 修改对其他线程立即可见 | volatile, synchronized, final  |
| 有序性 | 防止指令重排      | volatile(内存屏障), happens-before |

**happens-before 规则**(面试重点)：

1. 程序顺序规则：同一线程内，前面的操作 happens-before 后面的
2. 锁规则：unlock happens-before 后续 lock
3. volatile 规则：写 happens-before 后续读
4. 线程启动规则：start() happens-before 线程内所有操作
5. 线程终止规则：线程内所有操作 happens-before terminate()
6. 传递性：A happens-before B, B happens-before C → A happens-before C

**结合 SECP 业务场景**：

> 在 SECP 的 `TagMismatchCollector` 中：
> 
> - `AtomicReference` 的 value 字段是 volatile 的，保证一个线程 `getAndSet()` 替换 buffer 后，其他线程立即可见新 buffer。
> - `ConcurrentHashMap` 的 Node 数组用 volatile 修饰，保证一个线程写入的新节点对其他读线程立即可见。
> 
> 在结算模块的 Redisson 分布式锁中，`lock.lock()` 获取锁和 `lock.unlock()` 释放锁之间有 happens-before 关系——线程 A 解锁 happens-before 线程 B 加锁，保证 A 在锁内对结算单的修改对 B 可见。底层 Redisson 通过 Redis 的 Lua 脚本 + Redis 单线程模型保证原子性和可见性。

---

### 4.5 JVM 调优实战

**结合 SECP 业务场景的 JVM 调优**：

**场景一：设备数据管道 OOM 排查**

> **现象**：secp-prophet(告警服务)在高峰期(设备数据上报高峰)频繁 Full GC，导致告警延迟。
> 
> **排查过程**：
> 
> 1. `jstat -gc <pid> 1000`：观察 GC 频率，发现老年代占用率持续 > 90%
> 2. `jmap -histo:live <pid> | head -20`：查看对象统计，发现大量告警事件对象存活
> 3. `jmap -dump:format=b,file=heap.hprof <pid>` + MAT 分析：发现 Elasticsearch 请求的 Response 对象被缓存但没有及时释放
> 
> **解决**：
> 
> - ES 查询改用 Scroll API 分批拉取，避免一次性加载大量命中结果
> - 增加 -Xmx 从 4g 到 6g
> - 调整 -XX:InitiatingHeapOccupancyPercent=35 让 G1 更早触发 Mixed GC

**场景二：Arthas 在线诊断**

> SECP 使用 Arthas 进行线上问题诊断：
> 
> ```bash
> # 查看哪个方法最耗时
> trace com.goodwe.sebu.electricity.biz.service.settlement.impl.SettlementServiceImpl generateSettlement
> 
> # 查看方法入参出参
> watch com.goodwe.sebu.electricity.biz.service.bizfile.BizFileServiceImpl generateBizFile params+returnObj -x 3
> 
> # 查看线程状态
> thread -n 5  # CPU 占用最高的 5 个线程
> 
> # 查看类加载情况
> dashboard
> ```

**SECP 推荐 JVM 参数配置**：

```bash
# 基础配置
-Xms4g -Xmx4g                    # 堆大小固定，避免扩容抖动
-Xmn1536m                         # 新生代 3/8
-XX:MetaspaceSize=256m
-XX:MaxMetaspaceSize=512m

# G1 回收器
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:G1HeapRegionSize=16m
-XX:InitiatingHeapOccupancyPercent=45

# GC 日志
-Xloggc:/data/logs/gc/gc.log
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+PrintGCTimeStamps
-XX:+UseGCLogFileRotation
-XX:NumberOfGCLogFiles=10
-XX:GCLogFileSize=50M

# 堆外内存
-XX:MaxDirectMemorySize=1g        # Netty/ByteBuffer 堆外内存限制

# 故障诊断
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/data/logs/dump/  # OOM 时自动 dump
```

---

## 五、设计模式

> **pdai 知识体系**：设计模式分为三大类——创建型(对象的创建)、结构型(对象的组合)、行为型(对象的交互)。SECP 平台大量运用了 GoF 23 种设计模式中的 10+ 种，以下结合实际代码讲解。

### 5.1 策略模式 (Strategy)

**pdai 知识点回顾**：

> 策略模式定义一系列算法，将每一个算法封装起来，并使它们可以互相替换。策略模式让算法的变化独立于使用算法的客户端。

**标准 UML**：

```
┌──────────────┐      ┌──────────────────┐
│  Context     │─────>│  Strategy(接口)   │
│  -strategy   │      │  +algorithm()    │
│  +execute()  │      └────────┬─────────┘
└──────────────┘               │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
              ┌─────────┐ ┌─────────┐ ┌─────────┐
              │StrategyA│ │StrategyB│ │StrategyC│
              └─────────┘ └─────────┘ └─────────┘
```

**结合 SECP 业务场景（核心代码）**：

SECP 平台的 **Gaea 数据查询引擎** 大量使用策略模式。不同指标(tag)的数据查询逻辑完全不同，但调用入口统一：

```java
// === 策略接口 ===
// QueryNodeDataStrategy.java — 数仓原子数据查询策略
public interface QueryNodeDataStrategy {
    boolean isOpen();
    Set<String> getSupportedTags();
    List<UnifiedNodeMetricResponse> queryNodeData(String tag, UnifiedNodeMetricParam param);
    List<? extends AbstractQueryTagTask> buildQueryJob(String tag, Set<Long> nodeIds, ...);
}

// === 具体策略实现（每个策略处理不同的指标类型）===
// LegacyNodeDataStrategy.java — 旧版节点数据查询
public class LegacyNodeDataStrategy implements QueryNodeDataStrategy { ... }

// SimpleNodeDataStrategy.java — 简化版节点数据查询
public class SimpleNodeDataStrategy implements QueryNodeDataStrategy { ... }

// === 策略上下文（Context）===
// AtomicQueryNodeDataContext.java — 通过 Map 调度策略
@Component
public class AtomicQueryNodeDataContext {
    private final Map<String, QueryNodeDataStrategy> strategyMap;

    // 构造函数注入所有策略实现，按 tag 注册到 Map
    public AtomicQueryNodeDataContext(List<QueryNodeDataStrategy> strategies, ...) {
        this.strategyMap = new HashMap<>(strategies.size());
        for (QueryNodeDataStrategy strategy : strategies) {
            if (strategy.isOpen()) {
                for (String tag : strategy.getSupportedTags()) {
                    strategyMap.put(tag, strategy);
                }
            }
        }
    }

    public Map<Long, List<UnifiedMetricInfoData>> getNodesAtomicData(UnifiedNodeMetricReq req) {
        // 根据 tag 从 Map 取策略，提交到线程池并行执行
        List<QueryNodesDataTask> tasks = tags.stream()
            .map(tag -> new QueryNodesDataTask(tag, strategyMap.get(tag), req))
            .collect(Collectors.toList());
        // ... 并发执行各策略
    }
}
```

**另一个策略模式实例 — 电价模板策略**：

```java
// ElectricityTemplateTypeInterface.java — 电价模板策略接口
public interface ElectricityTemplateTypeInterface {
    void createPriceTemplate(AgreementInfo info, ElectricityPriceTemplateCreateReq req);
    void updatePriceTemplate(AgreementInfo info, ...);
    ElectricityPriceTemplateResp getDraftPriceTemplate(...);
    ElectricityPriceTemplateResp getValidPriceTemplate(AgreementInfo info);
}

// 7 个具体策略实现，对应 7 种电价模板类型：
// CustomElectricityTemplateTypeImpl      — 自定义电价
// FloatDiscountElectricityTemplateTypeImpl — 浮动折扣电价
// StateGridElectricityTemplateTypeImpl    — 国网电价
// WeightedElectricityTemplateTypeImpl     — 加权电价
// StorageCustomElectricityTemplateTypeImpl — 储能自定义电价
// StorageStateGridElectricityTemplateTypeImpl — 储能国网电价
// HangYuanCustomizedElectricityTemplateTypeImpl — 杭源定制电价
```

**面试加分回答**：

> "在 SECP Gaea 数据查询引擎中，我们用策略模式处理不同指标的查询。定义 `QueryNodeDataStrategy` 接口，每个指标类型一个策略实现，通过 Spring 构造函数注入所有策略，按 `getSupportedTags()` 注册到 `Map<String, Strategy>`。运行时根据 tag 从 Map 取策略，提交到线程池并行查询。这比 if-else 的好处是：新增指标只需加一个 `@Component` 实现类，不需要改 Context 代码，符合开闭原则。类似地，电费结算模块的 7 种电价模板也用 `ElectricityTemplateTypeInterface` 策略接口，不同客户签不同电价协议就用不同策略实现。"

---

### 5.2 模板方法模式 (Template Method)

**pdai 知识点回顾**：

> 模板方法模式在一个方法中定义算法骨架，将某些步骤延迟到子类实现。模板方法让子类在不改变算法结构的情况下，重新定义算法中的某些步骤。

**标准 UML**：

```
┌──────────────────────────┐
│  AbstractClass (抽象类)   │
│  +templateMethod() ◄── final，定义算法骨架
│  +primitiveOp1() ◄── abstract，子类实现
│  +primitiveOp2() ◄── abstract，子类实现
└──────────────────────────┘
            ▲
    ┌───────┴───────┐
    ▼               ▼
┌─────────┐   ┌─────────┐
│SubClassA│   │SubClassB│
└─────────┘   └─────────┘
```

**结合 SECP 业务场景（核心代码）**：

SECP 结算模块的 **订单生成器** 使用模板方法定义了生成流程骨架，子类只需实现差异化的周期判断逻辑：

```java
// AbstractOrderGenerator.java — 模板方法 + 责任链
public abstract class AbstractOrderGenerator implements IOrderGenerator {

    // === 模板方法：定义订单生成流程骨架 ===
    @Override
    public void doGenerate() {
        List<ContractSettlementView> matchedConfigs = matchedConfigs(); // 步骤1: 匹配配置
        if (CollUtil.isEmpty(matchedConfigs)) return;

        List<LocalDateTime> previousPeriods = previousPeriod();  // 步骤2: 获取上一周期（抽象方法）
        LocalDateTime startTime = previousPeriods.get(0);
        LocalDateTime endTime = previousPeriods.get(1);

        for (ContractSettlementView config : matchedConfigs) {
            doGenerate(config, startTime, endTime);  // 步骤3: 逐条生成
        }
    }

    @Override
    public void doGenerate(ContractSettlementView config, ...) {
        RLock lock = redissonClient.getReadWriteLock(...).writeLock(); // 步骤3a: 分布式锁
        try {
            if (lock.tryLock(5000, TimeUnit.MILLISECONDS)) {
                generate(config, startTime, endTime);  // 步骤3b: 核心生成逻辑
                lock.unlock();
            }
        } finally { ... }
    }

    // === 抽象方法：由子类实现差异化逻辑 ===
    // 子类需要实现: periodTypeEnum() 返回周期类型(月结/季结/年结)
    // 子类需要实现: previousPeriod() 返回上一周期时间范围
    // 子类需要实现: generate() 具体的生成逻辑
}
```

**另一个模板方法实例 — 消息推送处理器**：

```java
// AbstractCommonPostProcessor.java — 消息推送后处理器
public abstract class AbstractCommonPostProcessor implements IPostProcessor {

    // === 模板方法：定义消息处理流程骨架 ===
    public void process(PushMessage pushMessage) {
        if (checkPushTypes(pushMessage.getMessageInfo().getPushTypes())) {
            if (MessagePushTypeEnum.WEB.equals(getPushType())) {
                processContent(pushMessage);   // 抽象方法 ← 子类实现
                send(pushMessage);
            } else {
                filterUsers(pushMessage);      // 过滤接收用户
                if (CollUtil.isNotEmpty(filteredUsers)) {
                    processContent(pushMessage);   // 抽象方法 ← 子类实现
                    send(pushMessage);
                }
            }
        }
        if (nextProcessor != null) {            // 责任链传递
            nextProcessor.process(pushMessage);
        }
    }

    // === 抽象方法 ===
    protected abstract MessagePushTypeEnum getPushType();
    protected abstract void processContent(PushMessage pushMessage);
    protected abstract void processNotifyContent(PushMessage pushMessage);
    protected abstract void processScheduleNotifyContent(ScheduleNotifyContext ctx);
    protected abstract void processDirectNotifyContent(DirectNotifyContext ctx);
    protected abstract void processDelayedNotifyContent(DelayedNotifyContext ctx);
}
```

**面试加分回答**：

> "SECP 的订单生成器和消息推送处理器都用了模板方法模式。以 `AbstractOrderGenerator` 为例，它定义了 `doGenerate()` 模板方法：匹配配置→获取周期→分布式锁→核心生成。其中 `periodTypeEnum()` 和 `generate()` 是 abstract 方法，由月结、季结、年结等子类实现差异。这避免了重复代码（锁、配置匹配等公共逻辑在父类），同时保留了灵活性（子类只关心差异部分）。模板方法的好处是控制了算法骨架不变，子类只扩展可变部分，符合里氏替换原则。"

---

### 5.3 责任链模式 (Chain of Responsibility)

**pdai 知识点回顾**：

> 责任链模式将请求的发送者和接收者解耦，使多个对象都有机会处理请求。将这些对象连成一条链，沿着链传递请求，直到有一个对象处理为止。

**标准 UML**：

```
Client ──> ┌──────────┐ next ┌──────────┐ next ┌──────────┐
           │Handler A │ ───> │Handler B │ ───> │Handler C │
           │+handle() │      │+handle() │      │+handle() │
           └──────────┘      └──────────┘      └──────────┘
```

**结合 SECP 业务场景（核心代码）**：

SECP 告警服务（Prophet）的消息推送链路是典型的责任链模式：

```java
// ProcessorChain.java — 责任链组装
@Configuration
public class ProcessorChain {

    private final AbstractCommonPostProcessor processorChain;

    // 构造函数组装链: Email -> AppPush -> WebPush -> SMS
    public ProcessorChain(EmailPostProcessor emailPostProcessor,
                          AppPushPostProcessor appPushPostProcessor,
                          WebPushPostProcessor webPushPostProcessor,
                          SmsPostProcessor smsPostProcessor) {
        this.processorChain = emailPostProcessor;
        emailPostProcessor.setNextProcessor(appPushPostProcessor);
        appPushPostProcessor.setNextProcessor(webPushPostProcessor);
        webPushPostProcessor.setNextProcessor(smsPostProcessor);
    }

    public void process(PushMessage pushMessage) {
        processorChain.process(pushMessage);  // 从链头开始处理
    }
}

// AbstractCommonPostProcessor.java — 责任链节点（同时是模板方法）
public abstract class AbstractCommonPostProcessor implements IPostProcessor {

    private AbstractCommonPostProcessor nextProcessor;  // 下一个处理器

    public void setNextProcessor(AbstractCommonPostProcessor processor) {
        this.nextProcessor = processor;
    }

    public void process(PushMessage pushMessage) {
        // 当前处理器处理（模板方法）
        if (checkPushTypes(pushMessage.getMessageInfo().getPushTypes())) {
            // ... 处理逻辑
            processContent(pushMessage);  // 抽象方法
            send(pushMessage);
        }
        // 传递给下一个处理器
        if (nextProcessor != null) {
            nextProcessor.process(pushMessage);
        }
    }
}
```

**面试加分回答**：

> "SECP 告警服务的消息推送就是责任链模式。告警事件产生后需要根据用户配置推送到不同渠道：邮件→APP推送→Web推送→短信。我们定义 `AbstractCommonPostProcessor` 作为链节点基类，每个渠道(Email/AppPush/WebPush/SMS)继承它。在 `ProcessorChain` 配置类中按顺序组装链。处理时从链头开始，每个处理器检查是否需要推送该渠道，然后调用 `nextProcessor.process()` 传递给下一个。好处是：新增推送渠道只需加一个子类+改组装顺序，不需要修改其他处理器代码。而且责任链和模板方法在这里组合使用——每个节点用模板方法定义处理骨架，用责任链串联多个节点。"

---

### 5.4 代理模式 (Proxy)

**pdai 知识点回顾**：

> 代理模式为其他对象提供一种代理以控制对这个对象的访问。分为静态代理和动态代理，Spring AOP 底层使用 JDK 动态代理或 CGLIB。

**分类**：

| 类型         | 实现                              | SECP 场景            |
| ---------- | ------------------------------- | ------------------ |
| 静态代理       | 手写代理类                           | 较少使用               |
| JDK 动态代理   | `Proxy.newProxyInstance()`，基于接口 | Feign 客户端          |
| CGLIB 动态代理 | 字节码生成，基于继承                      | Spring AOP @Aspect |

**结合 SECP 业务场景（核心代码）**：

SECP 使用 Spring AOP 实现声明式业务锁，本质就是动态代理：

```java
// BusinessLockAspect.java — AOP 声明式分布式锁（代理模式应用）
@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
public class BusinessLockAspect {

    private final RedissonClient redissonClient;

    @Pointcut("execution(public * com.goodwe.sebu.electricity.biz.service..*.*(..))")
    private void serviceController() {}

    @Pointcut("@annotation(com.goodwe.sebu.electricity.biz.service.lock.BusinessLock)")
    private void anyAddLockMethod() {}

    @Pointcut("anyAddLockMethod() && serviceController()")
    public void effectPointcut() {}

    @Around("effectPointcut()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        // 1. 前置增强：获取分布式锁
        String lockEntityStr = String.valueOf(point.getArgs()[0]);
        BusinessLock businessLock = targetMethod.getAnnotation(BusinessLock.class);
        String lockKey = String.format(businessLock.lockFormat(), lockEntityStr);

        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(lockKey);
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            // 2. 目标方法执行
            res = point.proceed(point.getArgs());
        } finally {
            // 3. 后置增强：释放锁
            lock.unlock();
        }
        return res;
    }
}
```

**其他 AOP 代理在 SECP 中的应用**：

| AOP 切面                    | 功能                       | 模式      |
| ------------------------- | ------------------------ | ------- |
| `BusinessLockAspect`      | 声明式分布式锁                  | @Around |
| `FeignMonitorAspect`      | Feign 调用监控               | @Around |
| `IpLimiterAspect`         | IP 限流(Guava RateLimiter) | @Around |
| `OpLogAspect`             | 操作日志记录                   | @Around |
| `MessagePushLogAspect`    | 消息推送日志                   | @Around |
| `TryCatchAspect`          | 统一异常处理                   | @Around |
| `OperationLogAspect`      | 审计操作日志                   | @Around |
| `UnifiedTargetAuthAspect` | 统一目标鉴权                   | @Around |

**面试加分回答**：

> "SECP 用 Spring AOP 实现声明式业务锁——在方法上加 `@BusinessLock` 注解，`BusinessLockAspect` 切面通过 `@Around` 拦截，在方法执行前用 Redisson 加分布式锁，执行后释放。这就是代理模式的典型应用：Spring 为被切面的 Bean 生成 CGLIB 子类代理对象，调用时先走切面逻辑再调目标方法。好处是业务代码只需加注解，不用侵入锁逻辑。SECP 中有 8+ 个 AOP 切面覆盖锁、监控、限流、日志、鉴权等横切关注点，都用了代理模式。"

---

### 5.5 外观模式 (Facade)

**pdai 知识点回顾**：

> 外观模式为子系统中的一组接口提供一个一致的界面。外观模式定义了一个高层接口，使得子系统更容易使用。

**结合 SECP 业务场景**：

SECP 电费结算模块大量使用 Facade 模式，将复杂的多个子系统调用封装为一个统一入口：

```java
// ElectricFacadeServiceImpl.java — 发电量外观服务
@Service
public class ElectricFacadeServiceImpl implements ElectricFacadeService, ElectricityRealFacadeService {
    // 内部依赖：其他子系统的 Facade
    private final MidBizDataFacadeService midBizDataFacadeService;  // 中间层数据
    private final TagCheckProcessor tagCheckProcessor;             // 指标校验

    // 对外暴露简化的高层接口
    public Map<Long, Map<DateTime, EInfo>> batchGetAppointDayE(
            Collection<Long> stationIds, SectionDate sectionDate) {
        // 1. 调用子系统获取原始数据
        Map<Long, Map<DateTime, EInfo>> raw =
            midBizDataFacadeService.batchGetStationDayE(stationIds, sectionDate);
        // 2. 数据校验
        raw.values().forEach(dayEInfos -> dayEInfos.values().forEach(this::checkTag));
        return raw;
    }
}
```

**SECP 中的 Facade 统计（电费结算模块）**：

| Facade 接口                | 封装的子系统           | 对外提供的功能 |
| ------------------------ | ---------------- | ------- |
| ElectricFacadeService    | 数据中台 + 指标校验      | 发电量查询   |
| ProfitFacadeService      | 发电 + 电价 + 损耗     | 收益计算    |
| BigDataFacadeService     | 数据中台 + 算法引擎      | 大数据查询   |
| ContractSettlementFacade | 结算 + 协议 + 订单     | 合同结算    |
| SniperFacadeService      | 设备管理 + 电站信息      | 设备查询    |
| GaeaFacadeService        | 数据仓库 + 策略引擎      | 统一指标查询  |
| ManagerFacadeService     | 用户 + 租户 + 部门     | 组织管理    |
| MidBizDataFacadeService  | 中间层 + ClickHouse | 中间业务数据  |
| FacadeUnifiedTagService  | 统一标签 + 权限        | 统一标签管理  |

**面试加分回答**：

> "SECP 30+ 微服务之间相互调用非常复杂，我们用 Facade 模式做封装。比如 `ElectricFacadeServiceImpl` 对外只暴露 `batchGetAppointDayE()` 方法，内部封装了调用数据中台获取原始数据、指标校验器做数据质量检查等多个子系统调用。结算模块有 20+ 个 FacadeService，形成分层外观：上层(结算流程) → Facade 层(业务封装) → 底层(数据中台/算法/设备)。好处是上层不需要知道底层的复杂度，降低耦合。而且 Facade 天然适合微服务拆分——每个 Facade 可以独立拆成一个微服务。"

---

### 5.6 观察者模式 (Observer)

**pdai 知识点回顾**：

> 观察者模式定义对象间的一对多依赖关系，当一个对象状态发生变化时，所有依赖它的对象都会得到通知并自动更新。Spring 的事件机制(`ApplicationEvent` + `ApplicationListener`)是观察者模式的标准实现。

**标准 UML**：

```
┌────────────────┐  notify  ┌──────────────┐
│  Subject       │ ──────> │  Observer    │
│  -observers    │         │  +update()   │
│  +attach()     │         └──────────────┘
│  +detach()     │                  ▲
│  +notify()     │          ┌───────┴───────┐
└────────────────┘          ▼               ▼
                    ┌──────────┐    ┌──────────┐
                    │ObserverA │    │ObserverB │
                    └──────────┘    └──────────┘
```

**结合 SECP 业务场景（核心代码）**：

SECP 用 Spring `ApplicationEvent` 实现业务事件通知，解耦核心流程和后续处理：

```java
// === 事件定义 ===
// AddToWhitelistEvent.java — 电站加入白名单事件
public class AddToWhitelistEvent extends ApplicationEvent {
    private final Long stationId;
    public AddToWhitelistEvent(Object source, Long stationId) {
        super(source);
        this.stationId = stationId;
    }
}

// SettlementApproveEvent.java — 结算单审批事件
// RegenerateSettlementEvent.java — 重新生成结算单事件
// AgreementRelTenantUpdateEvent.java — 协议关联租户变更事件
// AgreementShareEvent.java — 协议分摊事件
// DeptOperateEvent.java — 部门操作事件

// === 事件发布（Subject）===
// MetricCompensationServiceImpl.java
@Autowired
private ApplicationEventPublisher applicationEventPublisher;

public void compensate(Long stationId) {
    // ... 指标补偿核心逻辑
    AddToWhitelistEvent event = new AddToWhitelistEvent(this, stationId);
    applicationEventPublisher.publishEvent(event);  // 发布事件
}

// SettlementServiceImpl.java
applicationEventPublisher.publishEvent(new SettlementApproveEvent(id, settlementData));
applicationEventPublisher.publishEvent(new RegenerateSettlementEvent(Collections.singleton(coverDTO)));

// === 事件监听（Observer）===
// 监听器通过 @EventListener 或 ApplicationListener 接收事件
// 事件处理与核心业务解耦：审批通过后触发后续流程（生成账单、通知等）
```

**SECP 中的事件清单**：

| 事件                              | 发布方                         | 触发场景    | 监听方     |
| ------------------------------- | --------------------------- | ------- | ------- |
| `SettlementApproveEvent`        | SettlementServiceImpl       | 结算单审批通过 | 账单生成/通知 |
| `RegenerateSettlementEvent`     | SettlementServiceImpl       | 重新生成结算单 | 重算流程    |
| `AddToWhitelistEvent`           | StationWhitelistServiceImpl | 电站加入白名单 | 算法引擎    |
| `RemoveFromWhitelistEvent`      | StationWhitelistServiceImpl | 电站移出白名单 | 算法引擎    |
| `AgreementRelTenantUpdateEvent` | RelatedPartyServiceImpl     | 协议关联方变更 | 权限更新    |
| `AgreementShareEvent`           | RelatedPartyServiceImpl     | 协议分摊变更  | 分摊计算    |
| `DeptOperateEvent`              | DeptInfoServiceImpl         | 部门操作    | 组织树更新   |

**面试加分回答**：

> "SECP 结算和告警模块广泛使用 Spring 事件机制实现观察者模式。比如结算单审批通过后，`SettlementServiceImpl` 发布 `SettlementApproveEvent`，后续的账单生成、通知推送等监听器自动响应。这比直接在审批方法里调账单生成服务的好处是：核心审批逻辑与后续处理解耦，新增后续处理只需加 `@EventListener` 不需要改审批代码。而且 Spring 事件默认是同步的(在发布线程执行)，我们可以通过 `@Async` + 线程池实现异步事件，不阻塞核心审批流程。"

---

### 5.7 单例模式 (Singleton)

**pdai 知识点回顾**：

> 单例模式确保一个类只有一个实例，并提供全局访问点。五种实现方式：

| 实现方式     | 线程安全 | 懒加载 | 说明                  |
| -------- | ---- | --- | ------------------- |
| 饿汉式      | ✅    | ❌   | 类加载即创建              |
| 懒汉式(双检锁) | ✅    | ✅   | `volatile` + 双重检查   |
| 静态内部类    | ✅    | ✅   | 利用类加载机制             |
| 枚举       | ✅    | ❌   | 最安全，防反射             |
| 容器单例     | ✅    | ✅   | Spring `@Component` |

**结合 SECP 业务场景（核心代码）**：

```java
// === 饿汉式单例：MapStruct 生成的 Mapper（编译期生成）===
// DayPfNodeInfoPoMapper.java — MapStruct 自动生成的转换器
@Mapper
public interface DayPfNodeInfoPoMapper {
    // 饿汉式单例：接口中定义 static final 实例，类加载即创建
    DayPfNodeInfoPoMapper INSTANCE = Mappers.getMapper(DayPNodeInfoPoMapper.class);

    DayPfNodeInfo convertTo(DayPfNodeInfoPo po);
    DayPfNodeInfoPo convertFrom(DayPfNodeInfo domain);
}

// 使用时：
DayPfNodeInfoPo po = DayPfNodeInfoPoMapper.INSTANCE.convertFrom(domain);
```

```java
// === 容器单例：Spring Bean（SECP 最常见的单例形式）===
@Component
public class AtomicQueryNodeDataContext {
    // Spring 容器保证全局唯一实例
    // Spring 默认 scope=singleton，在容器初始化时创建
}

// === 双检锁单例：RedissonClient 初始化（简化示意）===
// RedissonClient 在 SECP 中通过 Spring Boot Starter 自动配置
// 底层保证全局唯一 RedissonClient 实例
```

**面试加分回答**：

> "SECP 中单例模式有三种应用场景：第一是 MapStruct 生成的 PO Mapper 用饿汉式单例（`INSTANCE = Mappers.getMapper(...)`），编译期生成实现类，类加载即创建实例，线程安全且无性能损耗，全平台有 50+ 个 Mapper 单例。第二是 Spring `@Component` 容器单例，所有 Service、Facade、Context 都是单例，Spring 容器管理生命周期。第三是 RedissonClient，分布式锁客户端全局唯一。面试常问双检锁为什么要加 volatile——因为 `new` 操作不是原子的(分配内存→初始化→引用赋值)，可能重排序导致其他线程拿到未初始化的对象。"

---

### 5.8 工厂模式 (Factory)

**pdai 知识点回顾**：

> 工厂模式分为：简单工厂(静态工厂方法)、工厂方法(每个产品对应一个工厂)、抽象工厂(创建产品族)。

**结合 SECP 业务场景（核心代码）**：

```java
// === 简单工厂 + 策略模式：结算策略工厂 ===
// SettlementStrategyFactory.java
@Component
public class SettlementStrategyFactory {

    private final Map<String, SettlementStrategy> strategyMap;

    // 构造函数注入所有策略实现，按结算类型注册
    public SettlementStrategyFactory(List<SettlementStrategy> strategies) {
        this.strategyMap = strategies.stream()
            .collect(Collectors.toMap(
                SettlementStrategy::getSettlementType,
                strategy -> strategy));
    }

    // 工厂方法：根据类型获取策略
    public SettlementStrategy getStrategy(String settlementType) {
        return Optional.ofNullable(strategyMap.get(settlementType))
            .orElseThrow(() -> new BusinessException(...));
    }

    public boolean supports(String settlementType) {
        return strategyMap.containsKey(settlementType);
    }
}

// === 简单工厂：消息处理器工厂 ===
// MessageHandlerFactory.java
public class MessageHandlerFactory {
    // 根据消息类型获取对应的处理器
}

// === Spring 容器作为抽象工厂 ===
// SECP 所有 Bean 由 Spring 容器创建和管理
// ApplicationContext.getBean() 就是工厂方法
// @Configuration + @Bean 是声明式工厂方法
```

**面试加分回答**：

> "SECP 中工厂模式的核心应用是 `SettlementStrategyFactory`——它结合了简单工厂和策略模式。构造函数通过 Spring 注入所有 `SettlementStrategy` 实现，按 `getSettlementType()` 注册到 Map。调用方通过 `getStrategy(settlementType)` 获取对应策略。这本质是 Spring 提供的'容器工厂'能力：Spring IoC 容器本身就是最大的抽象工厂，负责创建所有 Bean 实例。我们只需要定义接口和实现类，Spring 帮我们做实例化和注入。"

---

### 5.9 建造者模式 (Builder)

**pdai 知识点回顾**：

> 建造者模式将一个复杂对象的构建与表示分离，使同样的构建过程可以创建不同的表示。Lombok 的 `@Builder` 注解是建造者模式最常见的使用方式。

**结合 SECP 业务场景（核心代码）**：

```java
// === Lombok @Builder：参数对象构建 ===
// SECP 中大量 DTO/Param 使用 Lombok @Builder
QueryThingParam queryThingParam = QueryThingParam.builder()
    .thingIds(new ArrayList<>(thingIdList))
    .build();

QueryTenantParam queryTenantParam = QueryTenantParam.builder()
    .ids(Arrays.asList(tenantId))
    .build();

QueryThingParam.builder()
    .thingCodes(new ArrayList<>(thingCodeList))
    .build();

// === Guava CacheBuilder：缓存构建 ===
// IpLimiterAspect.java
Cache<String, RateLimiter> limiterCache = CacheBuilder.newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

// === Spring Redis Jedis 配置：建造者模式 ===
// RedisConfig.java
jedisClientConfiguration = JedisClientConfiguration.builder()
    .usePooling()
    .poolConfig(jedisPoolConfig)
    .build();
```

**面试加分回答**：

> "SECP 中建造者模式主要用 Lombok `@Builder` 和 Guava `CacheBuilder`。Lombok 在编译期生成 Builder 类，对参数多的对象特别好用——比如 `QueryThingParam` 有 10+ 个可选参数，用 Builder 链式调用比构造函数清晰得多。Guava 的 `CacheBuilder` 是经典 Builder 模式，支持链式配置过期时间、最大容量等。Builder 和工厂的区别是：工厂关注'创建什么'，Builder 关注'怎么创建'——Builder 分步骤组装复杂对象。"

---

### 5.10 适配器模式 (Adapter)

**pdai 知识点回顾**：

> 适配器模式将一个类的接口转换为客户期望的另一个接口。适配器让原本接口不兼容的类可以一起工作。

**标准 UML**：

```
┌──────────────┐        ┌──────────────┐
│  Target      │        │  Adaptee     │
│  (目标接口)   │        │  (被适配者)  │
│  +request()  │        │  +specReq()  │
└──────┬───────┘        └──────┬───────┘
       │                       │
       └───────┬───────────────┘
               ▼
        ┌──────────────┐
        │  Adapter     │
        │  -adaptee    │
        │  +request()  │ → 内部调用 adaptee.specReq()
        └──────────────┘
```

**结合 SECP 业务场景（核心代码）**：

SECP 对接了多个第三方监控系统（大华、海康威视），通过适配器模式统一为 `ThirdPartyMonitorAdapter` 接口：

```java
// === 目标接口 ===
// ThirdPartyMonitorAdapter.java — 统一监控适配器接口
public interface ThirdPartyMonitorAdapter {
    Integer getDeviceOnlineStatus(String sn);
    List<DeviceChannelDetail> getDeviceChannelDetail(String sn);
    VideoLiveStreamInfo getLiveStream(String sn, Integer channel);
    HistoryRecordStreamInfo getHistoryRecord(String sn, ...);
}

// === 适配器1：大华 ===
// DaHuaMonitorAdapter.java
@Component("daHuaMonitorAdapter")
public class DaHuaMonitorAdapter implements ThirdPartyMonitorAdapter {
    private final DaHuaFacadeService daHuaFacadeService;  // 大华特有接口

    @Override
    public Integer getDeviceOnlineStatus(String sn) {
        // 调用大华特有API，转换为统一返回格式
        ApiResponseWrapper<DeviceInfoResponse> resp = daHuaFacadeService.getSingleDeviceDetail(sn);
        return Optional.of(resp).filter(ApiResponseWrapper::isSuccessful)
            .map(ApiResponseWrapper::getData).map(DeviceInfoResponse::getStatus)
            .orElseGet(() -> { ... return -1; });
    }
}

// === 适配器2：海康威视 ===
// HikvisionMonitorAdapter.java
@Component("hikvisionMonitorAdapter")
public class HikvisionMonitorAdapter implements ThirdPartyMonitorAdapter {
    private final HikvisionFacadeService hikvisionFacadeService;  // 海康特有接口

    @Override
    public Integer getDeviceOnlineStatus(String sn) {
        // 调用海康特有API，转换为统一返回格式
        ...
    }
}
```

**其他适配器在 SECP 中的应用**：

| 适配器                        | 被适配者(Adaptee)          | 目标接口(Target)             | 适配内容     |
| -------------------------- | ---------------------- | ------------------------ | -------- |
| `DaHuaMonitorAdapter`      | DaHuaFacadeService     | ThirdPartyMonitorAdapter | 大华视频监控   |
| `HikvisionMonitorAdapter`  | HikvisionFacadeService | ThirdPartyMonitorAdapter | 海康视频监控   |
| `SupportSyncDeptAdapter`   | 支持系统部门接口               | SupportBaseAdapter       | 部门同步     |
| `SupportSyncUserAdapter`   | 支持系统用户接口               | SupportBaseAdapter       | 用户同步     |
| `SupportSyncTenantAdapter` | 支持系统租户接口               | SupportBaseAdapter       | 租户同步     |
| `SupportTokenAdapter`      | 支持系统认证接口               | SupportBaseAdapter       | Token 认证 |
| `GetDwDataAdapterPlugin`   | 数仓数据接口                 | 数据适配插件接口                 | 数仓数据获取   |

**面试加分回答**：

> "SECP 需要对接大华和海康两个视频监控系统，它们的 API 完全不同。我们定义 `ThirdPartyMonitorAdapter` 统一接口，然后为每个厂商写一个适配器——`DaHuaMonitorAdapter` 内部调用大华的 `DaHuaFacadeService`，`HikvisionMonitorAdapter` 内部调用海康的 `HikvisionFacadeService`，都转换为统一的 `getDeviceOnlineStatus()`、`getDeviceChannelDetail()` 等方法。上层代码不需要知道底层用的是大华还是海康，通过 Spring 注入对应的适配器即可。适配器和策略的区别是：适配器解决'接口不兼容'（改接口），策略解决'算法可替换'（换实现）。"

---

### 5.11 设计模式在 SECP 中的综合应用

**设计模式组合使用矩阵**：

| 组合         | 模式                  | SECP 场景     | 说明                   |
| ---------- | ------------------- | ----------- | -------------------- |
| 策略 + 工厂    | Strategy + Factory  | Gaea 数据查询引擎 | 工厂选择策略，策略执行查询        |
| 模板方法 + 责任链 | Template + Chain    | 告警消息推送      | 模板方法定义处理骨架，责任链串联多渠道  |
| 代理 + 观察者   | Proxy + Observer    | AOP + 事件    | 切面拦截(代理) + 事件通知(观察者) |
| 外观 + 适配器   | Facade + Adapter    | 第三方系统对接     | 外观统一入口，适配器转换接口       |
| 单例 + 建造者   | Singleton + Builder | 配置对象        | 单例保证唯一，Builder 链式构建  |
| 策略 + 模板方法  | Strategy + Template | 电价模板        | 策略选择算法，模板方法定义流程      |

**面试高频问题**：

**Q: 你在工作中用到了哪些设计模式？**

> "SECP 平台用到了 10+ 种设计模式：
> 
> - **策略模式**：Gaea 引擎按指标类型选择查询策略，7 种电价模板
> - **模板方法**：订单生成器定义流程骨架，消息处理器定义处理流程
> - **责任链**：告警推送链(邮件→APP→Web→短信)
> - **代理模式**：8+ 个 AOP 切面(锁、监控、限流、日志)
> - **外观模式**：20+ 个 FacadeService 封装子系统
> - **观察者模式**：Spring ApplicationEvent 事件通知
> - **适配器模式**：大华/海康第三方监控对接
> - **单例模式**：MapStruct Mapper + Spring Bean
> - **工厂模式**：策略工厂选择实现
> - **建造者模式**：Lombok @Builder + Guava CacheBuilder"

**Q: 策略模式和工厂模式的区别？**

> "两者常组合使用但本质不同：工厂关注'创建什么对象'，策略关注'用什么算法'。在 SECP 中，`SettlementStrategyFactory`（工厂）根据结算类型创建/获取对应的 `SettlementStrategy`（策略）实例，然后由策略执行具体算法。工厂是'对象创建'，策略是'行为替换'。"

**Q: 代理模式和装饰器模式的区别？**

> "代理模式控制对象访问（加权限、加锁、加日志），装饰器模式增强对象功能（加缓存、加压缩）。结构相似但意图不同。SECP 的 AOP 切面是代理模式——不改变业务逻辑，只加控制逻辑(锁、监控)。如果是在文件流上加 BufferedInputStream 则是装饰器——增强功能。"

---

## 六、数据库（PostgreSQL / MySQL）

> **SECP 真实数据库技术栈**：
> 
> - **主数据库：PostgreSQL**，采用 **schema 分域**（`e_settlement_payment`、`algorithm`、`data_config`、`data_monitor`、`openapi_gw` 等，一个 PG 实例多个 schema 对应不同微服务域）
> - **时序存储：TimescaleDB**（PostgreSQL 时序扩展，设备 5min 功率曲线数据）
> - **ORM：MyBatis-Plus**（自研 persistence-starter 封装了分页插件）
> - **连接池：HikariCP**
> - **多数据源：dynamic-datasource**（`@DS` 注解切换 master/slave/slave2/tsdb）
> - **SQL 监控：p6spy**（JDBC 代理驱动，打印真实 SQL 与耗时）
> - **调度组件用 MySQL**：DolphinScheduler 的元数据库

### 6.1 索引原理（B+树）与最左前缀

**面试题：为什么用 B+ 树而不是 B 树/红黑树/Hash？**

- **vs B 树**：B+ 树非叶子节点不存数据、只存索引，单页（16KB）能放更多键，树更矮（3 层可支撑千万级数据），磁盘 IO 次数更少；叶子节点用双向链表连接，范围查询（`BETWEEN`、`ORDER BY`）只需顺序扫描。
- **vs 红黑树**：二叉树太高，千万数据需要 23+ 层，即 23 次磁盘 IO。
- **vs Hash 索引**：Hash 等值查询 O(1) 很快，但**不支持范围查询和排序**。

**SECP 真实索引设计**（来自 `secp-electricity-settlement-payment/scripts` DDL 脚本）：

```sql
-- 多租户场景：租户 ID 打头，保证"同一租户的协议列表"查询走索引
CREATE INDEX idx_agreement_info_tenant_id_agreement_name
    ON e_settlement_payment.agreement_info (tenant_id, agreement_name);

CREATE INDEX idx_custom_info_investor_tenant_id_custom_name
    ON e_settlement_payment.custom_info (investor_tenant_id, custom_name);

-- 通用模式：tenant_id + 业务字段 组成联合索引
CREATE INDEX idx_biz_file_tenant_id_biz_id_biz_type
    ON e_settlement_payment.biz_file (tenant_id, biz_id, biz_type);

-- 算法域：查询维度打头
CREATE INDEX day_pf_node_info_grid_day_idx
    ON algorithm.day_pf_node_info (grid_id, day);
CREATE INDEX metric_compensation_log_station_id_tp_idx
    ON algorithm.metric_compensation_log (station_id, exec_date);
```

**面试要点 — 最左前缀原则**：联合索引 `(tenant_id, agreement_name)` 中：

- `WHERE tenant_id = ?` ✅ 走索引
- `WHERE tenant_id = ? AND agreement_name = ?` ✅ 走索引
- `WHERE agreement_name = ?` ❌ 不走该索引（跳过了 tenant_id）
- `WHERE tenant_id = ? AND agreement_name LIKE '苏%'` ✅ 前缀匹配可走索引

**为什么 SECP 索引都以 tenant_id 打头？** SECP 是多租户 SaaS 平台，几乎所有业务查询都带租户隔离条件，把 tenant_id 放在联合索引最左侧，保证最高频查询路径都能命中索引——这是**用业务驱动索引设计**的典型例子，面试时主动讲出来很加分。

**关联概念**：

- **聚簇索引 vs 二级索引**：主键索引叶子节点存整行（聚簇），二级索引叶子存主键值，查非索引列需要**回表**。
- **覆盖索引**：如果查询的列全部包含在索引里（如 `SELECT tenant_id, agreement_name`），无需回表。`biz_file` 的 `(tenant_id, biz_id, biz_type)` 索引对"查某租户某业务的文件类型列表"就是覆盖索引。
- **索引失效场景**：对索引列使用函数/运算、隐式类型转换（`varchar` 列用数字比较）、前导 `%` 模糊查询、`OR` 连接非索引列。

---

### 6.2 事务与隔离级别

**面试题：讲讲事务的 ACID 和隔离级别**

| 隔离级别                             | 脏读  | 不可重复读 | 幻读                |
| -------------------------------- | --- | ----- | ----------------- |
| READ UNCOMMITTED                 | ✅   | ✅     | ✅                 |
| READ COMMITTED（PG 默认）            | ❌   | ✅     | ✅                 |
| REPEATABLE READ（MySQL InnoDB 默认） | ❌   | ❌     | ❌（InnoDB 通过间隙锁解决） |
| SERIALIZABLE                     | ❌   | ❌     | ❌                 |

- **脏读**：读到别人未提交的数据；**不可重复读**：同一事务两次读同一行结果不同（别人 UPDATE 了）；**幻读**：两次读同一范围行数变了（别人 INSERT 了）。
- **MVCC**（多版本并发控制）：InnoDB 每行隐藏 `trx_id`（事务ID）+ `roll_pointer`（回滚指针）指向 undo log，读操作走快照版本、不加锁，实现"读写不冲突"。PG 更彻底——写时直接把旧版本存进 `xmax`，真空进程 `VACUUM` 异步清理旧版本。

**SECP 真实事务代码**（`secp-algorithm` 服务）：

```java
// StationWhitelistServiceImpl：rollbackFor 指定回滚异常，noRollbackFor 排除校验异常
@Transactional(rollbackFor = Exception.class, noRollbackFor = ValidationException.class)
public void saveWhitelist(...) { ... }

// 常规写法：所有服务统一 rollbackFor = Exception.class
@Transactional(rollbackFor = Exception.class)
public void compensationMetric(...) { ... }
```

**面试要点 — 为什么写 `rollbackFor = Exception.class`？**  
Spring 默认只对 `RuntimeException` 和 `Error` 回滚，受检异常（如 `IOException`）默认**不回滚**。SECP 统一加 `rollbackFor = Exception.class` 防止受检异常吞掉回滚；`noRollbackFor = ValidationException.class` 则是业务语义——参数校验失败属于"预期内的业务拒绝"，不需要触发回滚污染事务日志。

**面试高频 — @Transactional 失效的 8 种场景**（本质都是"AOP 代理失效"，可呼应 [5.4 代理模式](#54-代理模式-proxy)）：

1. **同类内方法自调用**（`this.methodB()` 不走代理对象）——最常见！
2. 方法不是 `public`
3. 异常被 `try-catch` 吞了
4. 抛出受检异常但没配 `rollbackFor`
5. 类没被 Spring 管理（没加 `@Service`）
6. 多线程调用（子线程不在同一事务连接里，事务绑定在 `ThreadLocal` 上，呼应 [2.7 ThreadLocal](#27-threadlocal-与跨线程上下文传递)）
7. 数据库引擎不支持事务
8. 传播行为配置错误（如 `NOT_SUPPORTED`）

**大事务问题**：SECP 结算单生成流程刻意把"文件渲染（调外部导出服务）"放在事务外、用 `CompletableFuture` 并行执行（呼应 [2.2](#22-completablefuture-异步编排)），事务里只保留 DB 写操作——**面试可以讲：长事务会长期占用数据库连接（HikariCP 只有 30 个），并且拖长 MVCC 旧版本存活时间，我们通过"事务边界最小化 + 异步编排"来避免大事务**。

---

### 6.3 连接池 HikariCP

**面试题：为什么需要数据库连接池？HikariCP 为什么快？**

TCP 三次握手 + 认证 + 权限校验，建立一次 MySQL/PG 连接开销 5ms+，池化复用连接避免反复创建。

**HikariCP 快的三个原因**：

1. 使用 `ConcurrentBag` 无锁连接借用结构（借鉴 `ThreadLocal` 思路，线程优先复用自己用过的连接）
2. FastList 替代 ArrayList（免范围检查、尾删 O(1)）
3. 代理类用 `Javassist` 字节码生成（比反射快）

**SECP 真实配置**（nacos `middle-data-monitor.yaml`）：

```yaml
spring:
  datasource:
    type: com.zaxxer.hikari.HikariDataSource
    hikari:
      maximum-pool-size: 30      # 最大连接数
      minimumIdle: 10            # 最小空闲连接
      connectionTimeout: 30000   # 获取连接超时 30s（超时抛 SQLTransientConnectionException）
      idleTimeout: 60000         # 空闲连接回收时间 60s
      maxLifetime: 180000        # 连接最大存活时间 3min
```

**参数背后的思考**（面试加分点）：

- **`maxLifetime=180000`（3 分钟）必须小于数据库的 `wait_timeout`**（MySQL 默认 8 小时，但中间件/防火墙常裁剪到几分钟）：如果连接池里的连接先被数据库/网络设备单方面断开，应用拿到的是"死连接"，报 `Connection is not available`。HikariCP 提前主动淘汰，避免这个问题。且它会给 `maxLifetime` 加 2.5% 随机抖动，防止大批连接同时失效造成"惊群"。
- **池不是越大越好**：连接越多，数据库上下文切换和锁竞争反而加剧。经验公式：`连接数 = CPU 核数 × 2 + 有效磁盘数`。SECP 单服务 30 已是偏大的值，配合 6 个隔离线程池的 CallerRunsPolicy 背压（呼应 [2.1](#21-线程池-threadpoolexecutor-核心原理)），防止 DB 被打爆。

---

### 6.4 MyBatis / MyBatis-Plus

**面试题：`#{}` 和 `${}` 的区别？**

- `#{}` → **预编译参数占位符**，生成 `?`，值通过 `PreparedStatement.setString()` 传入，**防 SQL 注入**
- `${}` → **字符串直接拼接**，有注入风险，只用于 `ORDER BY ${column}` 这类无法参数化的场景（且必须白名单校验）

SECP 所有 Mapper XML 统一用 `#{}`，动态排序用枚举白名单。

**面试题：MyBatis 分页插件原理？**

MyBatis-Plus 的 `PaginationInnerInterceptor` 基于 **MyBatis 拦截器（Interceptor）机制**——本质是**动态代理 + 责任链**（呼应 [5.3 责任链](#53-责任链模式-chain-of-responsibility) 和 [5.4 代理模式](#54-代理模式-proxy)）：

```
Executor.query() 被代理
  → 拦截原始 SQL
  → 改写为 SELECT COUNT(*)（先查总数，超页则直接返回空）
  → 拼接 LIMIT offset, size（物理分页，不是内存分页）
  → 执行并包装成 Page 对象
```

SECP 在自研的 `secp-persistence-starter` 中封装了 `CustomPaginationInnerInterceptor`，并在 `NewMybatisPlusConfig` 中设置 `setOverflow(false)`（页码超界不回退到第 1 页而是返回空），统一全平台分页行为。

**面试题：批量插入怎么写？**

SECP 真实代码（`MessageStationRoleRelDao.xml`，一条 SQL 插多行，比循环单条 insert 快一个数量级）：

```xml
<insert id="insertBatch" keyProperty="id" useGeneratedKeys="true">
    insert into message_station_role_rel(user_id, message_rule_id, ...)
    values
    <foreach collection="entities" item="entity" separator=",">
        (#{entity.userId}, #{entity.messageRuleId}, ...)
    </foreach>
</insert>
```

**追问：`foreach` 拼几万行会怎样？** SQL 包过大（`max_allowed_packet`）、解析器变慢。MyBatis-Plus 的 `saveBatch` 默认每 1000 条 flush 一次，但 JDBC 默认**不是真批量**——MySQL 要加 `rewriteBatchedStatements=true` 才会把多条 insert 重写成一条多值 insert；PG 驱动用 `reWriteBatchedInserts=true`。

**面试题：Upsert（存在则更新，不存在则插入）怎么实现？**

SECP 两种数据库两种写法，都是真实代码：

```sql
-- MySQL 语法（secp-manager 消息角色关系表）
INSERT INTO message_station_role_rel(...) VALUES (...), (...)
ON DUPLICATE KEY UPDATE web = VALUES(web), app = VALUES(app), ...;

-- PostgreSQL 语法（middle-forward 天气数据表，指定冲突键）
INSERT INTO location_weather_now_today(...)
VALUES (...)
ON CONFLICT (location_id, hourly_time) DO UPDATE SET ...;
```

两者都依赖**唯一约束/主键**判断冲突，且是**原子操作**——比"先 SELECT 再 INSERT/UPDATE"少一次往返且无并发竞态。SECP 天气数据每小时定时拉取刷新，用 upsert 实现幂等写入（呼应场景二"幂等性"）。

---

### 6.5 多数据源与读写分离

**SECP 真实多数据源配置**（nacos `secp-data-config.yaml`，dynamic-datasource）：

```yaml
spring:
  datasource:
    dynamic:
      primary: dataConfig          # 默认数据源
      datasource:
        dataConfig:                # 业务配置库（PG schema: data_config）
          url: jdbc:p6spy:postgresql://10.197.14.7:5432/secp
          hikari: { maxPoolSize: 30, minIdle: 10, ... }
        tsdb:                      # 时序库（PG schema: tsdb）
          url: jdbc:postgresql://10.197.14.6:5432/secp
```

**代码层切换**（`@DS` 注解）：

```java
@DS("tsdb")     // 丢包点位查询走时序库
public class LossPacketPointRepositoryImpl implements LossPacketPointRepository { ... }

// middle-forward 天气服务：读走从库，写走主库
@DS("slave")  public List<WeatherHourly> selectHistory(...) { ... }
@DS("master") public void syncWeather(...) { ... }
```

**原理**（面试要点）：`@DS` 基于 **AOP 拦截 + ThreadLocal 存储数据源 key**（呼应 [2.7 ThreadLocal](#27-threadlocal-与跨线程上下文传递) 和 [5.4 代理模式](#54-代理模式-proxy)）：调用进入前把 key 塞进 `DynamicDataSourceContextHolder`（ThreadLocal），`AbstractRoutingDataSource`（Spring 提供的路由数据源抽象）的 `determineCurrentLookupKey()` 从 ThreadLocal 取 key 路由到真实 DataSource。

**追问：读写分离的主从延迟问题怎么解决？**

1. **写后立即读的场景强制走主库**（SECP 用 `@DS("master")` 显式指定）
2. 关键写操作后的读请求携带"会话标记"，中间件路由到主库
3. 业务上容忍短暂不一致（如报表、曲线展示）

**追问：@DS 和 @Transactional 一起用要注意什么？**  
事务开启时连接已绑定到当前线程（`DataSourceTransactionManager` 把 Connection 放进 ThreadLocal），事务内切换 `@DS` **不生效**——同一个事务里只能用一个数据源。需要跨库操作就拆成两个事务，或引入分布式事务（Seata AT 模式）。

---

### 6.6 时序数据与 TimescaleDB

**业务背景**：SECP 管理全球几十万台光伏设备，每台设备每 5 分钟上报功率/环境数据，写多读多、按时间范围查询——典型时序场景。

**SECP 真实 DDL**（`ddl_create_station_5min_data.sql`）：

```sql
CREATE TABLE algorithm.station_pv_power_5min_t0 (
    date_time  timestamp(6) NOT NULL,
    station_id bigint       NOT NULL,
    p          decimal      DEFAULT NULL,
    CONSTRAINT station_pv_power_5min_t0_pk PRIMARY KEY ("station_id", "date_time")
);

-- 转成 TimescaleDB 超表：按时间自动分区（chunk），1 天一个 chunk
SELECT create_hypertable('algorithm.station_pv_power_5min_t0',
                         'date_time', chunk_time_interval => INTERVAL '1 day');
```

**面试要点**：

- **为什么时序数据不适合普通表？** 几十亿行单表，B+ 树不断膨胀、索引维护代价高、旧数据删除（DELETE）代价极高且产生大量碎片。
- **hypertable 自动分区**：对应用透明（还是一张表），底层按 `date_time` 切成 1 天一个 chunk，**写入永远落在最新 chunk 的索引上**（B+ 树小、全部热缓存）；查"最近 7 天"只扫描 7 个 chunk（分区裁剪 pruning）；**删除旧数据 = 直接 DROP 整个 chunk**，秒级完成、无碎片。
- **主键设计 `(station_id, date_time)`**：一个电站一条时间序列，等值+范围查询（`WHERE station_id = ? AND date_time BETWEEN ? AND ?`）完全命中主键索引。
- **面试对比分库分表**：传统方案是对 MySQL 按 `station_id` 哈希分库、按时间分表（ShardingSphere），需要应用层路由、跨分片查询复杂；SECP 选择 PG + TimescaleDB，**用数据库原生分区能力替代分库分表**，海量写入下仍保持单库简洁性——这是一个很好的"技术选型"故事。

---

### 6.7 慢 SQL 排查与优化

**SECP 的排查工具链**：

1. **p6spy**：所有服务 JDBC URL 都包了 `jdbc:p6spy:postgresql://...`，p6spy 是 JDBC 代理驱动，**打印真实执行 SQL（含参数）+ 执行耗时**，测试环境定位慢 SQL 第一现场。
2. **PG `pg_stat_statements`** / MySQL `slow_query_log`（`long_query_time=1s`）：生产统计 Top N 慢 SQL。
3. **EXPLAIN（ANALYZE）**：看执行计划。

**EXPLAIN 重点看什么**（面试答这个顺序）：

1. **type/access method**：`Seq Scan`（全表扫描）→ 要优化；`Index Scan`/`Index Only Scan` → 正常
2. **rows**：预估行数和实际行数差太远 → 统计信息过期，`ANALYZE` 一下
3. **关键谓词/过滤条件**：Filter 里出现索引列 → 索引没命中（函数包装？类型转换？）
4. **Sort/临时文件**：排序 spills to disk → 考虑加索引消除排序或加大 `work_mem`

**优化 checklist**（按优先级）：

1. SQL 层：避免 `SELECT *`、消灭子查询里的 N+1（SECP 用 MyBatis-Plus `selectBatchIds` 代替循环单查）
2. 索引层：按查询模式建联合索引（tenant_id 打头）、用覆盖索引消灭回表
3. 量级层：大事务拆小、批量写入、分页深翻页改游标（`WHERE id > #{lastId} LIMIT n` 代替 `LIMIT 100000, 20`）
4. 架构层：读写分离、冷热分离（TimescaleDB chunk 压缩归档）、加缓存（Redis）

---

### 6.8 MySQL vs PostgreSQL

**面试可能追问：你们为什么用 PG 不用 MySQL？**

| 维度        | MySQL (InnoDB)               | PostgreSQL                                      | SECP 的选择                                                       |
| --------- | ---------------------------- | ----------------------------------------------- | -------------------------------------------------------------- |
| 事务实现      | undo log 回滚 + MVCC           | MVCC 原生多版本（旧版本在堆内，VACUUM 清理）                    | 两者都够用                                                          |
| 复杂查询      | 优化器较弱（早期版本）                  | 优化器强，支持 CTE、窗口函数早且全                             | 结算报表复杂聚合多                                                      |
| JSON 支持   | JSON 类型                      | **JSONB**（二进制存储、可索引 GIN）                        | `request_logs.additional_info jsonb` 存开放接口附加信息                 |
| 扩展性       | 插件少                          | **扩展生态**：TimescaleDB/PostGIS/pg_stat_statements | 时序数据必须 TimescaleDB                                             |
| 并发写       | 行锁 + 间隙锁                     | 无间隙锁概念（RR 靠可串行化快照 SSI）                          | —                                                              |
| upsert    | ON DUPLICATE KEY UPDATE      | ON CONFLICT ... DO UPDATE（可指定冲突键，更明确）           | 两种都在用                                                          |
| schema 概念 | database 即 schema，无独立 schema | **database → schema → table 三级**                | 一库多 schema 分域：`e_settlement_payment`/`algorithm`/`data_config` |

**一句话回答**：SECP 核心痛点是**海量设备时序数据**，TimescaleDB 是 PG 原生扩展，顺带获得了 JSONB、更强优化器和 schema 分域的微服务数据隔离能力；同时 PG 的 `ON CONFLICT` upsert 支撑了数据同步幂等写入。面试时展示"技术选型是业务驱动的"这个思维比背参数更重要。

**注意**：简历上如果写了 MySQL，面试官会问 MySQL 八股（InnoDB/redo log/undo log/binlog/两阶段提交）。补充速答：

- **redo log**（InnoDB，物理日志，保证 crash-safe 崩溃恢复，WAL）
- **undo log**（回滚日志 + MVCC 版本链）
- **binlog**（Server 层，逻辑日志，主从复制 + 数据恢复）
- **两阶段提交**：redo log prepare → 写 binlog → redo log commit，保证两个日志一致
- **一条 UPDATE 的完整流程**：Buffer Pool（不满则写 change buffer/脏页）→ 写 redo log（prepare）→ 写 binlog → 提交，后台线程刷脏页

---

## 七、Redis

> **SECP 真实 Redis 技术栈**：
> 
> - **客户端**：Spring Data Redis + Lettuce（nacos 配置 `lettuce.pool`，max-active 8）
> - **序列化**：统一 `StringRedisSerializer`（避免 JDK 序列化的乱码和跨语言问题）
> - **分布式锁**：Redisson（`lockWatchdogTimeout=10s` 看门狗续期）
> - **使用场景**：第三方 token 缓存（大华/OA）、分布式锁（海康初始化防重复）、Redis Pub/Sub 跨服务缓存失效、计数器

### 7.1 Redis 数据结构与底层实现

**面试高频 — Redis 为什么快？**

1. **纯内存**操作，无磁盘 IO
2. **单线程**命令处理（6.0 后 IO 多线程，命令执行仍单线程），无锁无竞争、无上下文切换
3. **IO 多路复用**（epoll），一个线程处理大量连接
4. 高效数据结构（SDS、跳表、压缩列表）

**5 种基础类型 + 底层**（必背）：

| 类型         | 底层结构                          | 典型场景        | SECP 应用                                        |
| ---------- | ----------------------------- | ----------- | ---------------------------------------------- |
| **String** | SDS（简单动态字符串）                  | 缓存、计数器、分布式锁 | Token 缓存 `opsForValue.get/set`、Redisson 锁的 key |
| **Hash**   | ziplist（小）/ hashtable（大）      | 对象存储        | —                                              |
| **List**   | quicklist（ziplist + 链表）       | 消息队列、最新列表   | —                                              |
| **Set**    | intset / hashtable            | 去重、交集       | —                                              |
| **ZSet**   | ziplist / **skiplist + dict** | 排行榜、延迟队列    | —                                              |

**追问：ZSet 为什么用跳表不用红黑树？**

- 跳表范围查询 O(logN + M)，只需沿底层链表遍历；红黑树范围查询要中序遍历，实现复杂
- 跳表实现简单（概率层）、并发友好、内存可调（p=1/4）

**追问：String 为什么不用 C 的 char[] 而用 SDS？**

- SDS 记录 `len` 和 `free`，**O(1) 获取长度**（strlen C 字符串要遍历 O(N)）
- **二进制安全**（不怕 `\0`），可存图片/序列化字节
- **空间预分配 + 惰性释放**，减少内存重分配

**SECP 序列化选择**（`RedisConfig.java`）：

```java
@Bean
public RedisTemplate<String, Object> redisTemplate(LettuceConnectionFactory connectionFactory) {
    RedisTemplate<String, Object> template = new RedisTemplate<>();
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new StringRedisSerializer());   // 不用 JdkSerializationRedisSerializer
    template.setHashKeySerializer(new StringRedisSerializer());
    template.setHashValueSerializer(new StringRedisSerializer());
    template.afterPropertiesSet();
    return template;
}
```

**面试要点**：Spring Boot 默认 `RedisTemplate` 用 `JdkSerializationRedisSerializer`，存进去的是 Java 对象序列化字节流，Redis 里看到的是乱码、`redis-cli` 无法直接查看、跨语言不兼容。SECP 统一换成 `StringRedisSerializer`，Value 存 JSON 字符串，可读、可跨语言、可 Debug——面试时讲"踩过的坑"很加分。

---

### 7.2 缓存三剑客（穿透 / 击穿 / 雪崩）

| 问题       | 现象                    | 解决方案                           | SECP 做法                                                             |
| -------- | --------------------- | ------------------------------ | ------------------------------------------------------------------- |
| **缓存穿透** | 查不存在的 key，每次都打到 DB    | ① 缓存空值（短 TTL） ② 布隆过滤器          | 缓存空值 + 参数校验拦截                                                       |
| **缓存击穿** | 热点 key 过期瞬间，大量请求打到 DB | ① 互斥锁（只放一个去查 DB） ② 热点 key 永不过期 | **Redisson 分布式锁**（呼应 [7.4](#74-分布式锁-redisson)），同一 key 只有一个线程查 DB 回填 |
| **缓存雪崩** | 大量 key 同时过期           | ① TTL 加随机值 ② 多级缓存 ③ 限流降级       | Token 缓存 TTL 跟随第三方接口 `expiresIn`，天然错开                               |

**SECP 真实代码 — 击穿防护**（`HikvisionComponent` 海康初始化，多 Pod 防重复消费）：

```java
// 分布式锁防止多 Pod 重复初始化
boolean isLock = redissonClient.getLock(RedisConstants.LOCK_HIKVISION_INIT_CONSUMER)
                              .tryLock(10, 20, TimeUnit.SECONDS);
// 锁内查 DB/初始化，锁外直接读缓存
Object cachePodObject = redisTemplate.opsForValue().get(RedisConstants.CACHE_HIKVISION_INIT_CONSUMER);
if (cachePodObject == null) {
    // 获取锁的 Pod 执行初始化，写回缓存
    redisTemplate.opsForValue().set(CACHE_KEY, CURRENT_POD, cacheTimeForPod, TimeUnit.SECONDS);
}
```

---

### 7.3 缓存与数据库一致性

**面试题：缓存和数据库怎么保证一致？**

四种策略对比：

| 策略               | 写顺序                                | 问题                     |
| ---------------- | ---------------------------------- | ---------------------- |
| 先更新 DB，再删缓存      | DB → DEL cache                     | 并发下可能缓存删了又被旧数据回填       |
| 先删缓存，再更新 DB      | DEL cache → DB                     | 删缓存后、DB 更新前有读请求把旧值写回缓存 |
| **延迟双删**         | DEL cache → DB → sleep → DEL cache | 推荐，覆盖大部分场景             |
| 订阅 binlog（Canal） | DB → Canal → DEL cache             | 最终一致，最稳                |

**SECP 的实际选择**：

- **Token 缓存场景**（`DaHuaFacadeServiceImpl`）：读多写少，第三方控制过期时间，不存在一致性问题
- **跨服务缓存失效**：用 **Redis Pub/Sub** 主动通知（呼应 [7.5](#75-redis-pubsub-轻量级消息)）——资产变更时发广播，消费方清自己的本地缓存

```java
// SECP 真实代码：先查缓存，缓存未命中查接口再回填
String redisDahuaToken = (String) redisTemplate.opsForValue().get(DAHUA_TOKEN_REDIS_KEY);
if (StrUtil.isBlank(redisDahuaToken)) {
    // 调用第三方接口获取 token
    DaHuaAuthResponse resp = daHuaAuthApi.auth(...);
    // 回填缓存，TTL 取接口返回的 expiresIn（提前 1 秒防边界）
    redisTemplate.opsForValue().set(DAHUA_TOKEN_REDIS_KEY, token, resp.getExpiresIn() - 1, TimeUnit.SECONDS);
}
```

**追问：为什么 token 缓存用 `expiresIn - 1` 而不是 `expiresIn`？**  
提前 1 秒过期，避免"缓存刚过期但旧 token 在第三方还没失效"的边界竞态——这是**防御性编程**的好例子，面试主动讲。

---

### 7.4 分布式锁 Redisson

**面试题：Redis 分布式锁怎么实现？Redisson 比 SETNX 好在哪？**

**基础版 SETNX 的问题**：

```
SETNX lock_key value       # 加锁
EXPIRE lock_key 30         # 设过期——非原子！中间宕机锁永不释放
```

→ 优化为 `SET key value NX EX 30`（原子），但仍有问题：**业务执行超时，锁自动释放，别的线程拿到锁，原线程回来误删别人的锁**。

**Redisson 的解决方案**（SECP 在用）：

```java
// SECP RedissonConfig：看门狗超时 10s
config.useSingleServer().setAddress(url).setPassword(...);
config.setLockWatchdogTimeout(10000);   // 看门狗默认续期间隔
```

1. **看门狗（Watchdog）自动续期**：`RLock.tryLock()` 不传 leaseTime 时，启动一个定时任务（每 `lockWatchdogTimeout/3` 秒），检查持有锁的线程还活着就续期到 `lockWatchdogTimeout`——**业务执行多久锁就持有多久，不会误释放**
2. **锁值带 UUID + 线程 ID**：解锁时 Lua 脚本判断 `value == 自己的 UUID:threadId` 才删——**不会误删别人的锁**
3. **可重入**：基于 Hash 结构记录重入次数
4. **支持公平锁、读写锁、信号量、CountDownLatch**（呼应 [2.5 AQS](#25-aqs-与-reentrantlock)）

**SECP 声明式锁**（`BusinessLockAspect`，AOP 注解化，呼应 [5.4 代理模式](#54-代理模式-proxy) 和 [2.3](#23-分布式锁-redisson-实现原理)）：

```java
@BusinessLock(key = "'settlement:' + #orderId", waitTime = 3, leaseTime = 10)
public void approveSettlement(Long orderId) { ... }
```

**追问：Redisson 锁的 CAP 取舍？**

- 单机 Redis：AP（主从切换丢锁）
- **RedLock**（多节点多数派）：CP，但有争议（Martin Kleppmann 批评）
- SECP 用单机 Redis + Redisson，对锁的强一致要求不高（结算幂等还有 DB 唯一约束兜底，呼应场景二）

---

### 7.5 Redis Pub/Sub 轻量级消息

**SECP 真实用法**——跨微服务的轻量级事件通知（比 MQ 轻、不需要持久化）：

```java
// 发送方：RedisMsgSendUtil
public void pub(String topic, Object message) {
    String msg = JacksonUtils.toJsonString(message);
    redisTemplate.convertAndSend(topic, msg);   // PUBLISH 命令
}

// 接收方：RedisListenerConfig 绑定 Topic → Listener
@Bean
public MessageListenerAdapter thingListenerAdapter(ThingListener thingListener) {
    return new MessageListenerAdapter(thingListener, "onMessage");
}
@Bean
public RedisMessageListenerContainer container(...) {
    listenerContainer.addMessageListener(thingListenerAdapter, new ChannelTopic(thingTopic));
    return listenerContainer;
}

// 消费方：ThingListener
public class ThingListener implements RedisReceiver {
    @Override
    @Transactional(rollbackFor = Exception.class, noRollbackFor = ValidationException.class)
    public void onMessage(String message) {
        ThingUpdateEvent event = JacksonUtils.fromJsonString(message, ThingUpdateEvent.class);
        contractSettlementFacade.refreshStationNames(event.getOriginal(), event.getNewThing());
    }
}
```

**面试要点 — Redis Pub/Sub vs MQ**：

| 维度   | Redis Pub/Sub      | Kafka / RocketMQ |
| ---- | ------------------ | ---------------- |
| 持久化  | ❌ 不持久（订阅者不在线消息丢失）  | ✅ 磁盘持久           |
| 可靠性  | 低（fire-and-forget） | 高（ACK + 重试）      |
| 延迟   | 微秒级                | 毫秒级              |
| 适用场景 | 缓存失效广播、实时通知        | 业务事件、数据管道        |

**SECP 选型逻辑**：物（设备）改名是低频操作、可容忍偶尔丢失、要求低延迟 → Redis Pub/Sub；设备数据采集、订单状态变更要求不丢 → RocketMQ/Kafka。

---

### 7.6 持久化与高可用

**面试速答**：

- **RDB**：全量快照（fork 子进程 `bgsave`），恢复快、文件小，但宕机丢最近一次快照后的数据
- **AOF**：追加写命令日志，`appendfsync always/everysec/no`，数据安全性高但文件大、恢复慢；4.0 后 **RDB+AOF 混合**（AOF 前半段是 RDB 全量 + 后半段增量命令）
- **主从复制**：全量（RDB 同步）+ 增量（repl_backlog 环形缓冲），读写分离
- **哨兵 Sentinel**：监控 + 自动故障转移（选主），保证高可用
- **Cluster**：16384 槽位分片（CRC16(key) % 16384），去中心化 gossip 协议

**SECP 部署**：单节点 Redis（nacos 配置单 host），业务对 Redis 可用性依赖可降级（缓存挂了走 DB），未上 Cluster——面试可讲"按需演进，不过度设计"。

---

## 八、消息队列（Kafka / RocketMQ）

> **SECP 真实 MQ 技术栈**：
> 
> - **RocketMQ**：业务事件驱动（资产分享联动、物操作联动、订单状态变更、延迟告警、测点补偿计算）
> - **Kafka**：大数据管道（数据仓库事件源、操作日志、用户行为日志、NiFi 数据流）
> - **自研封装**：`secp-rocketmq-starter`（`RocketMQSender` + `BaseMqMessageListener` 统一消息体 `MqMsg<T>`）

### 8.1 为什么用 MQ？三大作用与引入风险

**三大作用**：

1. **解耦**：结算服务发"订单状态变更"消息，多个下游各自消费，互不影响
2. **异步**：耗时操作（发邮件、推送 APP）扔到 MQ 异步处理，主流程快速返回
3. **削峰**：千万设备数据洪峰写入 Kafka，消费端按自己的速率消费

**引入风险**（面试必答，体现你不盲目用）：

1. **可用性下降**：MQ 挂了整个链路中断 → 降级方案（本地缓存 + 重试）
2. **复杂度上升**：消息丢失、重复、乱序、积压——每个都要处理
3. **一致性问题**：分布式事务（生产者发消息和本地 DB 操作要一致）

---

### 8.2 Kafka 核心原理

**面试题：Kafka 为什么吞吐量高？**

1. **顺序写磁盘**（append-only log），磁盘顺序写速度堪比内存随机写
2. **零拷贝**（sendfile，呼应 [3.2 零拷贝](#32-零拷贝技术)）：数据不进用户空间，从页缓存直接到 socket
3. **分区并行**：Topic 分多 Partition，消费者组内每个 Partition 一个消费者，水平扩展
4. **批量 + 压缩**：生产者攒一批 + gzip/snappy 压缩，减少网络往返

**核心概念**：

- **Topic → Partition → Offset**：消息按 Partition 有序（分区内有序，跨分区无序）
- **Consumer Group**：组内分摊消费，组间广播
- **Offset 提交**：自动（`enable.auto.commit`）vs 手动（`ack.acknowledge()`）

**SECP Kafka 生产者配置**（`ProphetNotifyPushKafkaConfig`）：

```java
props.put(ProducerConfig.ACKS_CONFIG, prophetNotifyKafkaProperties.getAck());       // acks=all，Leader+ISR 全确认
props.put(ProducerConfig.RETRIES_CONFIG, prophetNotifyKafkaProperties.getRetries()); // 重试次数
props.put(ProducerConfig.LINGER_MS_CONFIG, prophetNotifyKafkaProperties.getLingerMs()); // 攒批等待时间
props.put(ProducerConfig.BATCH_SIZE_CONFIG, prophetNotifyKafkaProperties.getBatchSize()); // 批大小
```

**面试要点 — `acks` 三档**：

- `acks=0`：不等确认，最快但可能丢
- `acks=1`：Leader 写入即确认，Leader 挂了丢数据
- `acks=all`（SECP 用）：Leader + 所有 ISR 副本都写入才确认，最安全。配合 `min.insync.replicas=2` 保证至少 2 个副本

**SECP Kafka 消费者**（`OperationLogMessageListener`，操作日志消费）：

```java
@KafkaListener(id = "operationLogKafkaListener", topics = {"${operation-log.kafka-cfg.topic}"},
    containerFactory = "operationLogKafkaConsumeFactory")
public void consumeOperationLogMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
    List<OperationLogPO> poList = records.stream().map(this::convert2PO).collect(Collectors.toList());
    operationLogRepository.batchInsert(poList);
    ack.acknowledge();   // 手动提交 offset，处理成功才提交
}

// 幂等去重：用 topic+partition+offset+timestamp 的 MD5 作为主键
private OperationLogPO convert2PO(ConsumerRecord<String, String> record) {
    String id = SecureUtil.md5(record.topic() + record.partition() + record.offset() + record.timestamp());
    ...
}
```

**面试亮点**：

1. **批量消费**（`List<ConsumerRecord>`）+ **批量插入** DB，吞吐量远高于逐条
2. **手动 ACK**（`ack.acknowledge()`）而非自动提交——处理失败不提交 offset，下次重投，保证不丢
3. **幂等去重**：用 `topic+partition+offset` 的 MD5 作为 DB 主键——Kafka 消息的"坐标"天然唯一，重投时主键冲突自动跳过，**这是 Kafka 幂等消费的经典做法**

---

### 8.3 RocketMQ 核心原理

**面试题：RocketMQ 和 Kafka 有什么区别？**

| 维度     | Kafka       | RocketMQ                  |
| ------ | ----------- | ------------------------- |
| 定位     | 日志/流处理大数据管道 | 业务消息                      |
| 延迟消息   | 不支持（需外部实现）  | **原生支持**（18 个 delayLevel） |
| 事务消息   | 不支持         | **原生支持**（半消息 + 回查）        |
| 消息重试   | 需自己实现       | **原生支持**（16 次递增重试 + 死信队列） |
| 消费模式   | 拉模式         | 推模式（本质是长轮询拉）              |
| Tag 过滤 | 不支持         | **支持**（Broker 端过滤）        |
| 顺序消息   | 分区内有序       | 分区内有序 + 支持分区路由            |

**SECP 为什么同时用两个？**

- **RocketMQ** 处理业务事件——需要延迟消息（资产分享延迟生效）、重试、死信队列、Tag 过滤
- **Kafka** 处理数据管道——操作日志、数据仓库事件源，高吞吐、批量、对接大数据生态

**SECP RocketMQ 配置**（nacos `secp-algorithm.yaml`）：

```yaml
rocketmq:
  name-server: 10.197.14.16:9876
  producer:
    group: calc_metric_compensation_producer_group
    send-message-timeout: 3000       # 发送超时 3s
  consumer:
    group-prefix: secp-algorithm     # 消费组前缀（环境隔离）
  env-isolation:                     # 环境隔离（test/prod 用不同 topic 后缀）
    enabled: true
    name: test
```

**SECP 生产者**（`RocketMqSenderService`，同步发送 + 延迟发送）：

```java
// 同步发送：等 Broker 确认才返回，保证可靠
SendResult result = rocketMQSender.syncSend(topic, MqMsg.of(message, "无功补偿"));

// 延迟发送：1 秒后投递（资产分享场景，给数据写入留缓冲时间）
rocketMQSender.syncDelaySend(topic, MqMsg.of(event), MessageDelayLevel.ONE_SECOND);
```

**SECP 消费者**（`OrderStatusChangeListener`，Tag 过滤 + 状态机消费）：

```java
@RocketMQMessageListener(
    topic = "${esp.rocketmq.order.status.change.topic}",
    selectorType = SelectorType.TAG,                          // TAG 过滤
    selectorExpression = "${esp.rocketmq.order.status.change.tag}",
    consumerGroup = "${esp.rocketmq.order.status.change.consumer-group}")
public class OrderStatusChangeListener implements RocketMQListener<MidOrderStatusChangeEvent> {
    @Override
    public void onMessage(MidOrderStatusChangeEvent event) {
        switch (MidOrderStatusEnum.lookupForCode(event.getStatus())) {
            case PAY_SUCCESS:   orderService.updatePayResult(...); break;  // 支付成功
            case PAYING:
            case CANCELED:      return;  // 无效状态直接跳过
            case ORDER_EXPIRED:
            case PAY_FAILED:    orderService.updatePayResult(...); break;  // 失败
        }
    }
}
```

**面试亮点**：用 **Tag 做消息路由**（同一 Topic 下不同 Tag 路由到不同消费者），用 **switch 状态机**消费（忽略无效状态、区分成功/失败），体现消息驱动 + 业务状态机设计的结合。

---

### 8.4 消息可靠性：不丢 / 不重 / 顺序

**① 不丢消息**：

| 环节     | 措施                   | SECP 做法                                    |
| ------ | -------------------- | ------------------------------------------ |
| 生产者    | 同步发送 + 重试 + acks=all | `syncSend` + `RETRIES_CONFIG`              |
| Broker | 持久化 + 副本             | Kafka acks=all；RocketMQ 同步刷盘 + 主从同步        |
| 消费者    | 手动 ACK（处理成功才确认）      | Kafka `ack.acknowledge()`；RocketMQ 消费成功才返回 |

**② 不重复（幂等）**：

MQ 的"至少一次"语义（At-Least-Once）意味着可能重复投递，**消费端必须幂等**：

- **唯一键去重**：SECP 操作日志用 `MD5(topic+partition+offset)` 做 DB 主键，重复投递主键冲突自动跳过
- **业务状态机**：SECP 订单消费用 switch 状态机，重复消息查当前状态已是终态则跳过
- **Redis 标记**：消费前 `SETNX msgKey 1`，已消费则跳过

**③ 顺序消息**：

- **全局有序**：单 Partition/单 Queue（牺牲并行度，很少用）
- **分区有序**：同一业务 key（如 orderId）的消息路由到同一 Partition，分区内有序（SECP 用 RocketMQ 的 MessageQueueSelector 按 orderId 取模选 Queue）

**④ 消息积压处理**：

1. 临时扩消费者（Partition 数 = 消费者上限，超过无意义）
2. 临时换 Topic 做扇出（一个消费者 → N 个新 Topic → N 组消费者）
3. 降级非核心消费

**⑤ 死信队列（DLQ）**：  
RocketMQ 消费失败 16 次重试后进入 `%DLQ%consumerGroup` 死信 Topic，SECP 通过监控告警 + 人工补偿处理。

---

### 8.5 MQ 在 SECP 中的实际应用

**场景一：资产分享联动（RocketMQ 延迟消息 + 多服务联动）**

```
secp-electricity-settlement-payment（你写的代码）
  → AssetShareUpdateProducer.syncDelaySend(topic, event, ONE_SECOND)
     ↓ 1 秒延迟（等 DB 事务提交）
secp-gaea
  → AssetShareMsgConsumer 消费
     → 按 shareMsgType 分发：CREATE / UPDATE / ENABLE / DISABLE
     → 调 SniperAdapter 查拓扑、写组织架构、更新数据权限
```

**面试讲解点**：① 延迟 1 秒解决"事务未提交消息已发出"的竞态 ② 多服务通过 MQ 解耦联动 ③ 消费端按消息类型做策略分发（呼应 [5.1 策略模式](#51-策略模式-strategy)）

**场景二：物操作联动（RocketMQ + BaseMqMessageListener 模板）**

```
secp-sniper（设备服务）发 thing-operate 消息
  → secp-algorithm ThingMsgConsumer 消费（删除白名单）
  → middle-openapi ThingOperateMessageListener 消费（同步第三方）
```

**面试讲解点**：① 自研 `BaseMqMessageListener<T>` 模板基类封装统一反序列化 + 异常处理 + 日志（呼应 [5.2 模板方法](#52-模板方法模式-template-method)） ② 同一消息多服务各自消费（Consumer Group 不同 = 广播）

**场景三：操作日志管道（Kafka 批量消费 + 幂等）**

```
各服务 AOP 拦截操作 → 发 Kafka → secp-manager 批量消费 → batchInsert 到 DB
幂等：MD5(topic+partition+offset) 做主键
```

**场景四：数据仓库事件源（Kafka 多 Topic 分流）**

secp-prophet 用 7+ 个 `@KafkaListener` 分别消费 dwh event/status/error/recovery 等不同 Topic，每个 Topic 对应一种设备事件类型——**用 Topic 做事件类型隔离，消费端独立扩展**。

---

## 九、Spring / Spring Boot / Spring Cloud

### 9.1 IOC 与 Bean 生命周期

**pdai 知识点回顾**：IOC（控制反转）把对象创建和依赖注入交给容器，DI 是实现方式。Bean 生命周期：实例化 → 属性填充 → Aware 回调 → BeanPostProcessor 前置 → 初始化（@PostConstruct / InitializingBean / init-method）→ BeanPostProcessor 后置（AOP 代理在这里生成）→ 使用 → 销毁。

**SECP 真实场景 — 自研 Starter 的生命周期钩子**：

公司把公共能力下沉到 `goodwe-common-parent`（含 `goodwe-core-spring-boot-starter`、`goodwe-persistence-spring-boot-starter` 等），所有 30+ 微服务引入即用：

```java
// goodwe-core-spring-boot-starter 的 spring.factories
org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
  com.goodwe.sebu.core.starter.config.methodlog.MethodLogAutoConfiguration,\
  com.goodwe.sebu.core.starter.config.swagger.SwaggerConfig,\
  com.goodwe.sebu.core.starter.config.weblog.config.WebLogConfig,\
  com.goodwe.sebu.core.starter.config.TraceIdFilterConfig,\
  com.goodwe.sebu.core.starter.operationlog.OperationLogAutoConfiguration,\
  com.goodwe.sebu.core.starter.aop.TryCatchAspect
```

> 面试讲生命周期时要能点到"BeanPostProcessor 是 Spring 扩展的命脉"：AOP 代理（BusinessLockAspect 生效的前提）、@ConfigurationProperties 绑定、自定义注解增强，全部基于它。项目里的 `OperationLogAutoConfiguration` 就是通过 BeanPostProcessor 扫描 @OperationLog 注解生成代理。

### 9.2 循环依赖与三级缓存

**pdai 知识点回顾**：三级缓存——singletonObjects（成品）、earlySingletonObjects（半成品）、singletonFactories（工厂）。A/B 互依时，A 实例化后先暴露 ObjectFactory 到三级缓存，B 通过工厂提前拿到 A 的引用（若是 AOP 则由工厂返回代理）。

**高频追问**：

- 为什么是三级不是两级？第三级存 ObjectFactory 是为了**延迟决定返回原对象还是代理对象**，只有真正发生循环依赖才提前创建代理，保证正常流程代理仍在初始化后生成（生命周期语义不变）。
- 为什么构造器注入无法解决？实例化阶段就需要的依赖，还没走到"暴露工厂"那一步。
- Spring Boot 2.6+ 默认禁止循环依赖——方向是**重构**（事件解耦、setter 改造），不是 `allow-circular-references=true`。

**SECP 实践**：结算服务里结算单生成（SettlementService）依赖模板服务、模板服务又依赖结算配置——通过 `ApplicationEventPublisher` 发 `SettlementApproveEvent` 事件解耦（观察者模式），从根上避免循环依赖。

### 9.3 AOP：JDK 动态代理 vs CGLIB

**pdai 知识点回顾**：接口 → JDK Proxy（反射实现 InvocationHandler）；无接口 → CGLIB（生成子类覆写方法）。Spring Boot 2.x 默认全 CGLIB。注解来自 AspectJ，但运行时是 Spring 自己的代理实现。

**SECP 真实场景 — 8+ 个 AOP 切面撑起横切能力**：

```java
// BusinessLockAspect.java — @BusinessLock 声明式分布式锁
@Around("@annotation(businessLock)")
public Object around(ProceedingJoinPoint pjp, BusinessLock businessLock) {
    RLock lock = redissonClient.getLock(key); ...
}
```

项目内切面清单：BusinessLockAspect（分布式锁）、TryCatchAspect（统一异常兜底）、WebLogConfig（出入参日志）、MethodLogAutoConfiguration（方法级日志）、操作日志切面、鉴权切面、监控切面……

**高频追问**：@Transactional 自调用失效的根因就是 AOP——this.method() 走的是原对象不是代理；解决办法：注入自身代理（AopContext.currentProxy()）或拆 Bean。

### 9.4 Spring 事务：传播与失效

已在 [6.2 事务与隔离级别](#62-事务与隔离级别) 结合 `StationWhitelistServiceImpl` 详述：`rollbackFor + noRollbackFor` 真实用法、8 大失效场景、大事务治理。此处不重复，面试时主动引用数据库章节内容。

### 9.5 Spring Boot 自动装配原理

**pdai 知识点回顾**：@SpringBootApplication → @EnableAutoConfiguration → 读取 META-INF/spring.factories（2.7+ 改为 `AutoConfiguration.imports`）加载候选配置类，经 @ConditionalOnXxx 过滤后注册 Bean。

**SECP 真实场景 — 公司自研 Starter 全流程**（面试可直接讲）：

1. **建 module**：`goodwe-core-spring-boot-starter`，命名遵循官方 `xxx-spring-boot-starter` 规范；
2. **写 AutoConfiguration**：TraceIdFilterConfig、TokenRedisConfig、FeignProperty 等配置类，用 @ConditionalOnProperty/@ConditionalOnMissingBean 控制；
3. **注册**：`src/main/resources/META-INF/spring.factories` 写入 EnableAutoConfiguration=配置类全限定名；
4. **业务服务只引依赖**：30+ 服务零代码获得 traceId、统一异常、操作日志、Feign 拦截等能力。

> 加分点：讲清楚"为什么用 @ConditionalOnMissingBean"——允许业务方覆盖默认实现（Starter 提供默认值，业务可定制），这是 Spring Boot "约定优于配置 + 可覆盖"的体现。

### 9.6 Spring Cloud 组件在 SECP 的落地

**pdai 知识点回顾 + SECP 对应**：

| 组件            | 解决什么        | SECP 落地                                                                                                               |
| ------------- | ----------- | --------------------------------------------------------------------------------------------------------------------- |
| Nacos         | 注册中心 + 配置中心 | 全部服务注册到 Nacos；配置集中在 `nacos_config_export`（数据源、线程池、MQ 参数全部配置化，支持动态刷新）                                                  |
| OpenFeign     | 声明式 HTTP 调用 | `ManagerServiceFeign`、`SniperServiceFeign` 等 20+ Feign 接口；自定义 `TraceIdInterceptor`（RequestInterceptor）把 traceId 透传到下游 |
| Gateway / 网关层 | 路由、鉴权、灰度    | middle-route-gw 路由网关 + `secp-gray-rule-extproc`（基于 Envoy ext_proc 的灰度规则扩展，Go 实现）                                      |
| 负载均衡          | 客户端 LB      | Ribbon/Spring Cloud LoadBalancer，配合 Nacos 服务列表                                                                        |
| 熔断限流          | 服务防雪崩       | 网关层降级 topic（route_gateway_degradation_topic）+ RocketMQ 补偿                                                             |

**Feign 细节追问**：

- Feign 本质：JDK 动态代理 + HTTP 客户端（默认 JDK URLConnection，可换 OkHttp/Apache HttpClient），把接口方法翻译成 HTTP 请求（编码器/解码器）。
- 超时与重试配置在 yml；项目里对跨网络调用（大华、海康）单独配置更长的 readTimeout。
- Feign 调用会丢失请求头（traceId、token），项目统一在 `TraceIdInterceptor` / `SecurityInterceptor`（starter 里）补齐——**这就是"自定义 RequestInterceptor"的一手经验**。

### 9.7 @Async 异步与 MDC 上下文传递

**pdai 知识点回顾**：@Async 本质是 AOP + 线程池提交；默认线程池是 SimpleAsyncTaskFactory（不复用线程），必须自定义 Executor。

**SECP 真实场景 — TaskDecorator 传递 traceId**：

```java
// MdcTaskDecorator.java（goodwe-core-starter）
public Runnable decorate(Runnable runnable) {
    Map<String, String> map = MDC.getCopyOfContextMap();  // 主线程的 MDC 快照
    return () -> {
        try {
            if (map != null) MDC.setContextMap(map);       // 复制到异步线程
            runnable.run();
        } finally {
            MDC.clear();                                    // 防线程池复用导致串号
        }
    };
}
```

> 这段代码同时回答三道面试题：① @Async 怎么自定义线程池（AsyncConfig 里 threadPoolTaskExecutor.setTaskDecorator）；② MDC 日志链路在异步线程为什么丢（ThreadLocal 隔离）怎么解决（装饰器复制）；③ 为什么 finally 里必须 clear（线程复用，不清会把上一个请求的 traceId 带给下一个请求）。micro-grid 服务的 `CustomMdcTaskDecorator`、algorithm 的 `AsyncConfig` 均有同款实现。

### 9.8 Spring 高频问答速答

- **Spring Bean 线程安全吗？**——单例 Bean 本身不保证；SECP 做法是无状态 Service（不写成员变量），需要状态就用 ThreadLocal / 并发容器 / 原子类（见第二章）。
- **@Autowired vs @Resource**：前者 Spring 提供、byType 优先；后者 JSR-250、byName 优先。
- **BeanFactory vs FactoryBean**：前者是容器；后者是"造 Bean 的 Bean"，getObject() 返回产物（FeignClientFactoryBean 就用它生成 Feign 代理）。
- **@Component vs @Bean**：类扫描 vs 方法注册，第三方库的类只能 @Bean。
- **事件机制**：ApplicationEvent + @EventListener，同步默认；结合 @Async 变异步（项目 SettlementApproveEvent 等 7 个业务事件）。

---

## 十、微服务与分布式

### 10.1 服务拆分：SECP 的域划分

SECP（智慧能源云平台）30+ 服务按业务域拆分：manager（用户/租户）、prophet（消息通知）、sniper（物模型/设备拓扑）、gaea（数据查询）、watchman（安防监控）、micro-grid（微网）、electricity-settlement-payment（电费结算）、algorithm（算法计算）、run-sentinel（运行巡检）、workflow-engine（工作流）、template-center（模板中心）……

**拆分原则（结合项目讲）**：按业务域（DDD 限界上下文）、团队边界、数据自治（每服务独立 PG schema）、独立伸缩（结算报表服务 CPU 密集，单独扩容）。

### 10.2 服务间通信选型

**SECP 三种通信方式的真实分工**：

| 方式                    | 场景       | 例子                  |
| --------------------- | -------- | ------------------- |
| OpenFeign 同步          | 需要实时结果   | 结算服务调 manager 查租户费率 |
| MQ 异步（RocketMQ/Kafka） | 事件通知、削峰  | 资产分享联动、操作日志管道       |
| Redis Pub/Sub         | 轻量、允许丢消息 | 物改名后通知各服务清缓存        |

**选型口诀**：要结果用 Feign，要可靠用 MQ，图轻快用 Pub/Sub——面试官问"为什么不统一用 MQ"，答案是过度异步会让调用方拿不到结果、排查链路变长。

### 10.3 分布式事务：SECP 为什么不用 Seata

**pdai 知识点回顾**：2PC/AT/TCC/Saga/本地消息表/最大努力通知。

**SECP 实际方案 — 最终一致性三板斧**：

1. **本地事务 + 事件**：结算审批在本地事务内完成状态变更，事务提交后发 `SettlementApproveEvent`；
2. **延迟消息防竞态**：资产分享服务用 `syncDelaySend(topic, msg, MessageDelayLevel.ONE_SECOND)` 延迟 1 秒发消息——**避免"事务未提交、下游已消费"的脏读竞态**（AssetShareUpdateProducer，本人编写）；
3. **补偿机制**：middle-route-gw 配置了 `route_gateway_compensation_producer_group` + 降级 topic，网关转发失败进补偿队列重试。

> 面试标准回答："我们评估过 Seata AT 模式，但它对业务侵入低的同时带来全局锁性能损耗、TC 单点等问题；我们的业务（结算、联动、通知）天然可接受秒级最终一致，所以选择'本地事务 + 事务后事件 + RocketMQ 延迟消息 + 补偿表'的组合，没有引入分布式事务框架的复杂度。"——能说出**为什么不用**比背 Seata 原理更加分。

### 10.4 链路追踪：自研轻量方案

**SECP 实现（goodwe-core-starter 内）**：

```
TraceIdFilter（入口，读 header 或生成 UUID → MDC.put）
    ↓
MdcTaskDecorator（@Async 线程传递）
    ↓
TraceIdInterceptor（Feign RequestInterceptor，MDC.get → 透传 header 到下游）
    ↓
secp-context-carrier-agent（Java Agent 字节码增强，线程池提交时自动复制上下文）
```

**与 SkyWalking 对比**：自研 traceId 只做"日志串联"（问题定位够用、零依赖）；SkyWalking 提供调用拓扑、耗时分析、存储分析（需 OAP+存储集群）。项目演进方向：日志串联自研 + APM 按需引入。

> 加分点：说得出 MDC 底层是 ThreadLocalMap（见 2.7），所以跨线程必须显式复制；而 Agent 方案在 Runnable 提交处字节码增强，业务零改造。

### 10.5 幂等设计汇总

| 层次  | 手段                               | SECP 例子                     |
| --- | -------------------------------- | --------------------------- |
| 接口层 | token/分布式锁                       | BusinessLockAspect（业务锁注解）   |
| 消费层 | MD5(topic+partition+offset) 标记去重 | OperationLogMessageListener |
| 状态机 | 状态流转校验，重复消息自然拒绝                  | OrderStatusChangeListener   |
| 数据库 | 唯一约束 / ON CONFLICT               | PG upsert                   |

### 10.6 分布式 ID

**pdai 知识点回顾**：UUID（无序，B+ 树页分裂）、雪花（时间戳+机器+序列，时钟回拨问题）、号段、数据库自增/序列。

**SECP 实际**：PG 主键用 BIGSERIAL/序列（单库内自增，天然有序）；跨服务无全局 ID 强需求（各域内部自洽 + 业务键如结算单号由 OrderGeneratorHolder 按规则生成，见 2.3/5.8）。面试可答："如果有全局 ID 需求会引入 Leaf/雪花，注意时钟回拨用等待或扩展位解决。"

### 10.7 高频问答

- **CAP 选型**：Nacos 注册中心 AP、配置中心 CP；Zookeeper CP；Eureka AP。
- **服务雪崩怎么防**：网关限流 + 熔断降级（降级 topic）+ 线程池隔离（6 个隔离池，见 2.1）+ 超时兜底。
- **灰度发布怎么做**：secp-gray-rule-extproc 基于 Envoy ext_proc 按规则（用户/租户/百分比）路由灰度 Pod。

---

## 十一、计算机网络

### 11.1 TCP 三次握手 / 四次挥手

**pdai 知识点回顾**：握手 SYN → SYN+ACK → ACK（确认双方收发能力、协商初始序列号）；挥手 FIN → ACK → FIN → ACK（全双工两个方向分别关）。TIME_WAIT 2MSL（可靠终止 + 旧报文消亡）。

**结合 SECP/MQTT 讲**：每台逆变器/采集器与 MQTT Broker（EMQX）建立的就是 TCP 长连接。MQTT CONNECT 之前必先完成 TCP 三次握手；心跳 keepalive 依赖 TCP keepalive 之上的应用层 PINGREQ/PINGRESP。百万设备长连接场景下，TIME_WAIT 过多会耗尽端口——这也是设备侧用长连接（避免反复建连）而非短连接的原因。

**高频追问速答**：

- 为什么不是两次？——服务端无法确认自己发的 SYN+ACK 客户端收到了；两次握手会让历史 SYN（网络滞留）建立无效连接。
- 为什么挥手四次？——TCP 全双工，被动方收到 FIN 时可能还有数据要发，ACK 和 FIN 分开发。
- 大量 CLOSE_WAIT 说明什么？——代码没调 close()，被动关闭后句柄泄漏，是**应用 bug** 而非网络问题。

### 11.2 TCP vs UDP

|     | TCP                   | UDP                |
| --- | --------------------- | ------------------ |
| 连接  | 面向连接                  | 无连接                |
| 可靠性 | 确认+重传+排序              | 尽力而为               |
| 场景  | MQTT（基于 TCP）、HTTP、RPC | CoAP（受限设备）、DNS、音视频 |

**结合 SECP**：光伏设备用 MQTT/TCP（指令下发必须可靠）；部分低功耗采集场景行业里用 CoAP/UDP——设备毫秒级唤醒上报后立即休眠，省去握手开销。

### 11.3 TCP 粘包/拆包

**根因**：TCP 是字节流协议，没有消息边界。Nagle 优化 + 接收缓冲区导致多次写合并/一次读被拆。

**解决**：定长、分隔符（\r\n）、长度字段（LengthFieldBasedFrameDecoder）。MQTT 协议自带剩余长度字段（varint 编码），Broker 解析天然按帧——面试可以此说明"应用层协议设计如何对抗粘包"。

### 11.4 HTTPS / TLS 握手

**pdai 知识点回顾**：ClientHello（随机数+套件）→ ServerHello（随机数+证书）→ 证书验证 → 密钥交换（ECDHE）→ 对称加密通信。非对称只交换密钥，数据传输用对称（性能）。

**SECP 真实代码 — MQTT over TLS 的信任链构建**：

```java
// MqttConfiguration.java — prophet 服务
trustStore.setCertificateEntry("Custom CA", CertificateFactory.getInstance("X509")
    .generateCertificate(HttpRequest.get(mqttConfigProperties.getCaFileOssUrl()).execute().bodyStream()));
TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
tmf.init(trustStore);
sslContext.init(null, trustManagers, null);   // 只装 TrustManager → 单向认证
```

> 一段代码讲透 TLS 概念：CA 证书（从 OSS 下载自签 CA）→ KeyStore/TrustManagerFactory（信任链校验）→ SSLSocketFactory 装配到 MQTT 连接。追问"双向认证呢"——再装 KeyManager（客户端证书+私钥），设备侧同理。

### 11.5 HTTP 版本演进与 REST

- **HTTP/1.1**：keep-alive 长连接、管道化（队头阻塞）；**HTTP/2**：二进制分帧、多路复用、头部压缩——OpenAPI 网关（middle-openapi-gw）面向第三方高频调用，2 减少连接数；**HTTP/3**：QUIC（UDP）解决 TCP 层队头阻塞。
- **RESTful 设计**：项目 Controller 统一 `/rpc/xxx`（服务间）、`/api/v1/xxx`（网关）路径规范 + 统一 Result<T> 响应体（starter 的 HttpMessageConfig 统一序列化）。
- **GET/POST 本质**：一个语义/幂等约定，一个提交数据；加密参数用 POST + body。

### 11.6 从输入 URL 到页面展示（速答骨架）

DNS 解析 → TCP 握手 → TLS 握手 → HTTP 请求 → 网关（路由/鉴权/限流）→ 服务处理（Feign/MQ/DB）→ 响应渲染。**用 SECP 的请求链路答这题是降维打击**：浏览器 → middle-route-gw（灰度路由）→ 服务 → Nacos 服务发现 → Feign 调下游（traceId 透传）→ PG/TimescaleDB → 返回。

---

## 十二、Netty 与 MQTT（设备接入）

### 12.1 Netty 是什么 & Reactor 模型

**pdai 知识点回顾**：Netty = 基于 NIO 的异步事件驱动网络框架。主从 Reactor：Boss EventLoopGroup（1-2 线程，只管 accept）→ 注册到 Worker EventLoopGroup（默认 CPU*2 线程，处理读写）。一个 Channel 绑定一个 EventLoop（线程绑定，无锁化）。

**为什么设备网关都用 Netty**：EMQX（项目使用的 MQTT Broker）底层即基于 Erlang/OTP（同理 Reactor 思想），Java 侧自建设备网关（IoT 公司标配）也用 Netty 承载百万长连接——单机几十万连接只需几十个线程。

### 12.2 Netty 核心组件速答

| 组件              | 作用                       | 高频追问                                                                                             |
| --------------- | ------------------------ | ------------------------------------------------------------------------------------------------ |
| Channel         | 连接抽象                     | Channel 与 EventLoop 绑定，handler 内无需加锁                                                             |
| EventLoop       | 事件循环（selector+taskQueue） | IO 与定时任务同队列，勿在 handler 阻塞（会拖死该 loop 上所有连接）                                                       |
| ChannelPipeline | handler 责任链              | 入站/出站方向相反，编解码即入站 Decoder/出站 Encoder                                                              |
| ByteBuf         | 字节容器                     | vs ByteBuffer：读写双指针、引用计数、池化（PooledByteBufAllocator，jemalloc 思想）、堆外内存；泄漏用 ResourceLeakDetector 检测 |
| ChannelFuture   | 异步结果                     | addListener 回调避免阻塞 eventLoop                                                                     |

**零拷贝补充**（呼应 3.2）：Netty 的"零拷贝"还指 CompositeByteBuf（逻辑合并不物理拷贝）、slice/duplicate（共享底层数组）。

### 12.3 MQTT 协议核心

**pdai/协议知识**：发布订阅、主题（支持通配符 +/ #）、QoS 0/1/2、遗嘱消息（Will）、保留消息（Retained）、keepalive 心跳。

| QoS | 语义          | SECP 场景                  |
| --- | ----------- | ------------------------ |
| 0   | 至多一次（可能丢）   | 高频遥测数据（5min 功率曲线，丢一帧可接受） |
| 1   | 至少一次（可能重）   | 一般业务消息（消费端幂等兜底）          |
| 2   | 恰好一次（四次挥手式） | 计费相关指令（重=多扣钱不可接受）        |

**通配符订阅**：`secp/{tenantId}/+/power` 订阅某租户全部场站功率；# 是多层通配。

### 12.4 SECP 中的 MQTT 代码

**出站（服务 → 设备）**：

```java
// MqttConfiguration.java — Spring Integration + Paho
@Bean
@ServiceActivator(inputChannel = UPGRADE_ANNOUNCEMENT_CHANNEL_NAME_OUT)
public MessageHandler mqttOutbound() {
    MqttPahoMessageHandler handler = new MqttPahoMessageHandler(
        clientId + System.currentTimeMillis(), mqttClientFactory());
    messageHandler.setAsync(mqttConfigProperties.getAsync());   // 异步发送
    messageHandler.setDefaultQos(mqttConfigProperties.getQos()); // QoS 配置化
    return handler;
}
```

**要点串讲**（面试可直接背）：

- 升级公告走 MQTT 广播到全部网关设备（prophet 服务的 UpgradeAnnouncementFacade）；
- 远程控制（RemoteControlServiceImpl）指令下发同样走 MQTT，响应走设备上报 topic，服务端订阅回执后状态机流转；
- 高可用：`setServerURIs(url.split(","))` 配多个 Broker 地址，Paho 自动重连 + 清洁会话配置；
- TLS：见 11.4 的 SSLContext 代码；
- **为什么用 Paho/Spring Integration 而不直接 Netty**：业务服务只需收发消息，Broker（EMQX）已承担连接管理、会话保持、QoS 重传——**自己写 Netty 的场景是自建 Broker/私有协议网关**，分清楚这一层边界非常加分。

### 12.5 高频问答

- **百万长连接怎么优化**：单机连接数（文件句柄 ulimit、内存 per-channel 开销）、EventLoop 数量、业务逻辑与 IO 线程分离（业务线程池，呼应 2.1 的 6 个隔离池）、心跳剔除僵尸连接（IdleStateHandler）。
- **Netty 心跳**：IdleStateHandler(readerIdleTime,...) 触发 userEventTriggered，超时 close。
- **TCP 长连接 vs 轮询**：设备侧必须长连接（省流量、低延迟、支持反向控制）；HTTP 轮询浪费且延迟高。

---

## 十三、Java 8+ 新特性

> **pdai 知识体系**：Java 8 是 Java 历史上最大的变化，Lambda + Stream + Optional + 新时间 API 彻底改变了 Java 编程风格。SECP 全平台 **6300+ 个 Java 文件** 使用了 Stream/Lambda/Optional，是日常开发的核心技能。

### 13.1 Lambda 表达式与函数式接口

**pdai 知识点回顾**：

> Lambda 是函数式接口（只有一个抽象方法的接口）的实例，本质是匿名内部类的语法糖。

**核心函数式接口**：

| 接口                  | 抽象方法              | 参数→返回       | 示例                         |
| ------------------- | ----------------- | ----------- | -------------------------- |
| `Function<T,R>`     | `R apply(T)`      | T → R       | `map(s -> s.length())`     |
| `Consumer<T>`       | `void accept(T)`  | T → void    | `forEach(s -> print(s))`   |
| `Supplier<T>`       | `T get()`         | () → T      | `() -> new HashMap()`      |
| `Predicate<T>`      | `boolean test(T)` | T → boolean | `filter(s -> s != null)`   |
| `BiFunction<T,U,R>` | `R apply(T,U)`    | (T,U) → R   | `compute(k, (k,v) -> v+1)` |

**结合 SECP 业务场景**：

SECP 中 Lambda 最密集的场景是设备数据批量处理：

```java
// CacheRedisServiceImpl.java — 设备列表批量提取并去重
List<String> deviceNodeIds = deviceResult.getData().getDataList().stream()
    .filter(thing -> thing.getNodeId() != null)   // Predicate Lambda
    .map(ThingDetailDto::getNodeId)                // Function Lambda（方法引用）
    .map(Object::toString)
    .distinct()
    .collect(Collectors.toList());

// SinperServiceImpl.java — 设备状态转 Map（BiFunction）
deviceStatusDTOList.stream().collect(Collectors.toMap(
    DeviceStatusDTO::getSn,                         // key mapper
    Function.identity(),                             // value mapper = 自身
    (existing, replacement) -> replacement));         // merge function（重复 key 取新值）
```

**面试加分回答**：

> "SECP 中 6000+ 文件使用 Lambda，最典型的场景是从设备列表中 filter + map + distinct 提取需要的字段。Lambda 的本质是函数式接口的实现——`filter` 的参数是 `Predicate<T>`，`map` 的参数是 `Function<T,R>`。在 `SinperServiceImpl` 中，我们用 `Collectors.toMap` 的第三个参数（merge function）处理设备 SN 重复的情况，这是一个 `BiFunction`，取新值覆盖旧值。面试常问 Lambda 和匿名内部类的区别：Lambda 只能实现函数式接口（单方法），但不需要编译生成额外的 class 文件，JDK 8 用 `invokedynamic` 指令在运行时动态生成，性能更好。"

---

### 13.2 Stream API

**pdai 知识点回顾**：

> Stream 不是数据结构，是对集合的函数式操作管道。分为中间操作（lazy）和终端操作（eager）。

**操作分类**：

```
创建 Stream
  ├── stream() / parallelStream()
  ├── Stream.of(...) / Arrays.stream(...)
  └── Stream.generate() / Stream.iterate()

中间操作（lazy，不触发执行）
  ├── filter(Predicate)      — 过滤
  ├── map(Function)          — 映射
  ├── flatMap(Function)      — 扁平化
  ├── distinct()             — 去重
  ├── sorted() / sorted(Comparator) — 排序
  ├── peek(Consumer)         — 查看（调试用）
  ├── limit(n) / skip(n)    — 截取
  └── mapToInt/mapToDouble   — 基本类型流

终端操作（eager，触发执行）
  ├── collect(Collector)     — 收集为集合
  ├── forEach(Consumer)      — 遍历
  ├── reduce(BinaryOperator) — 归约
  ├── count() / min() / max() — 聚合
  ├── anyMatch / allMatch / noneMatch — 匹配
  ├── findFirst / findAny   — 查找
  └── toArray()              — 转数组
```

**结合 SECP 业务场景**：

```java
// DataCenterServiceImpl.java — 设备 SN 批量提取
List<String> sns = newDataList.stream()
    .map(item -> item.getSn())                    // 提取 SN
    .collect(Collectors.toList());                // 收集为 List

// BlackListServiceImpl.java — 黑名单 IP 提取并去重
blackList.addAll(blackPOList.stream()
    .map(BlackListPO::getItemId)
    .distinct()                                    // 去重
    .collect(Collectors.toList()));

// ValidatorUtils.java — 参数校验错误信息格式化
String errorMsg = constraintViolations.stream()
    .map(result -> String.format("%s:%s", result.getPropertyPath(), result.getMessage()))
    .collect(Collectors.joining(";"));             // 拼接为字符串

// TokenRedisDao.java — Redis Key 集合转换
Set<String> keys = allWebUserRedisKey.stream()
    .map(Object::toString)
    .collect(Collectors.toSet());

// RelatedPartyServiceImpl.java — 按租户分组（保持顺序）
Map<Long, List<RoleInfo>> grouped = list.stream()
    .collect(Collectors.groupingBy(
        RoleInfo::getTenantId,
        LinkedHashMap::new,                        // 保持插入顺序
        Collectors.toList()));
```

**Stream 高频面试追问**：

| 问题                            | 回答                                                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Stream 是懒加载的吗？                | 中间操作是 lazy 的，只有终端操作触发时才执行。`peek` 可以验证这一点。                                                                  |
| parallelStream 用了什么线程池？       | `ForkJoinPool.commonPool()`，所有 parallelStream 共享，可能互相阻塞。SECP 不用 parallelStream，用独立线程池 + CompletableFuture。 |
| Stream 和 for 循环性能对比？          | 小数据量 for 更快（无创建 Stream 开销）；大数据量 + 复杂流水线 Stream 可利用短途优化（如 limit + filter 不遍历全量）。SECP 日常用 Stream 主要是可读性优先。   |
| `Collectors.toMap` key 重复会怎样？ | 默认抛 `IllegalStateException: Duplicate key`。需要传第三个参数 merge function 指定策略。SECP 中用 `(old, new) -> new` 取新值。   |
| Stream 能复用吗？                  | 不能，Stream 是一次性的，终端操作后不能再使用，否则抛 `IllegalStateException: stream has already been operated upon or closed`。   |

---

### 13.3 Optional 与空指针防御

**pdai 知识点回顾**：

> Optional 是一个容器对象，可能包含非空值也可能为空。设计目的是在 API 层面显式表达"可能为 null"，减少 NPE。

**常用方法**：

| 方法                       | 说明                       |
| ------------------------ | ------------------------ |
| `Optional.of(T)`         | 包装非空值，T 为 null 抛 NPE     |
| `Optional.ofNullable(T)` | 包装可能为 null 的值            |
| `Optional.empty()`       | 创建空 Optional             |
| `.isPresent()`           | 是否有值                     |
| `.get()`                 | 获取值（不安全，不推荐直接用）          |
| `.orElse(T)`             | 有值返回值，无值返回默认值            |
| `.orElseGet(Supplier)`   | 有值返回值，无值调 Supplier 生成默认值 |
| `.orElseThrow(Supplier)` | 有值返回值，无值抛异常              |
| `.ifPresent(Consumer)`   | 有值则执行 Consumer           |
| `.map(Function)`         | 有值映射，无值返回空 Optional      |
| `.filter(Predicate)`     | 有值且满足条件则返回，否则空 Optional  |

**结合 SECP 业务场景**：

```java
// StationIndicatorEnum.java — 枚举查找返回 Optional
public static Optional<StationIndicatorEnum> lookupForAlgorithmAttribute(String attr) {
    return Arrays.stream(values())
        .filter(e -> e.getAlgorithmAttribute().equals(attr))
        .findFirst();                                 // 返回 Optional<StationIndicatorEnum>
}
// 调用方
lookupForAlgorithmAttribute(algorithmAttribute)
    .map(StationIndicatorEnum::getDownstreamIndicator);  // 链式映射

// StationIndicatorInfoData.java — Optional 链式取值
public Optional<String> getValue() {
    return Optional.of(stationIndicatorInfo)
        .map(StationIndicatorInfo::getValue);           // 链式安全取值
}

// SettlementStrategyFactory.java — Optional 替代 if-null-throw
public SettlementStrategy getStrategy(String type) {
    return Optional.ofNullable(strategyMap.get(type))
        .orElseThrow(() -> new BusinessException(...));  // 无值抛业务异常
}

// BizFileServiceImpl.java — CompletableFuture + Optional 优雅降级
CompletableFuture<Optional<FileOssInfoResp>> excelFuture =
    CompletableFuture.supplyAsync(() -> {
        try {
            return Optional.of(templateCenterAdapter.writeToOSS(...));
        } catch (Exception e) {
            return Optional.empty();                    // Excel 模板不存在不阻断主流程
        }
    }, writeOssExecutor);
```

**面试标准回答**：

> "Optional 的核心价值是在 API 签名层面表达'可能为空'，而不是靠注释或口口相传。SECP 中三种典型用法：一是枚举查找返回 `Optional<Enum>`，调用方用 `.map()` 链式取属性；二是策略工厂 `Optional.ofNullable(map.get(type)).orElseThrow()`，一行代码替代 if-null-throw 三行；三是 CompletableFuture + Optional 组合——Excel 文件生成可能因模板缺失失败，用 Optional.empty() 优雅降级不影响 PDF/PNG 生成。**不推荐**直接调 `.get()` 或 `.isPresent() + .get()`，那和 null 检查没有区别。"

---

### 13.4 方法引用

**pdai 知识点回顾**：

> 方法引用是 Lambda 的进一步简化，当 Lambda 体只调用一个已有方法时，可以用 `::` 语法引用。

**四种方法引用**：

| 类型         | 语法         | 示例                    |
| ---------- | ---------- | --------------------- |
| 静态方法引用     | `类名::静态方法` | `Integer::parseInt`   |
| 实例方法引用（对象） | `对象::方法`   | `System.out::println` |
| 实例方法引用（类）  | `类名::方法`   | `String::length`      |
| 构造方法引用     | `类名::new`  | `HashMap::new`        |

**结合 SECP 业务场景**：

```java
// SECP 中最常见的方法引用
.map(ThingDetailDto::getSn)           // 实例方法引用：等价于 thing -> thing.getSn()
.map(ThingDetailDto::getNodeId)        // 同上
.map(Object::toString)                 // 同上
.distinct()
.collect(Collectors.toList());

// 构造方法引用
.collect(Collectors.groupingBy(
    RoleInfo::getTenantId,              // 方法引用做 classifier
    LinkedHashMap::new,                 // 构造方法引用做 map factory
    Collectors.toList()));

// SupplierWrapper + 方法引用（CompletableFuture 场景）
CompletableFuture.supplyAsync(
    SupplierWrapper.of(() -> ...),       // 这里用 Lambda，因为逻辑复杂
    writeOssExecutor);
```

---

### 13.5 接口默认方法与静态方法

**pdai 知识点回顾**：

> JDK 8 允许在接口中定义 `default` 方法和 `static` 方法，解决了接口演进问题——新增方法不会破坏已有实现类。

```java
// SECP 中的实际例子
public interface BaseMapper<S, T> {
    // 抽象方法
    T convertTo(S source);
    S convertFrom(T target);

    // 默认方法：批量转换（JDK 8 新特性）
    default List<T> convertToList(List<S> sourceList) {
        if (sourceList == null) return Collections.emptyList();
        return sourceList.stream()
            .map(this::convertTo)
            .collect(Collectors.toList());
    }
}

// MapStruct Mapper 继承 BaseMapper，自动获得批量转换能力
@Mapper
public interface NodeLastPackageInfoConverter extends BaseMapper<BigDataNodeLastPackageInfo, NodeLastPackageInfo> {
    NodeLastPackageInfoConverter INSTANCE = Mappers.getMapper(NodeLastPackageInfoConverter.class);
}
```

**面试追问**：

- 一个类继承两个接口，两个接口有相同的 default 方法怎么办？→ 编译报错，必须在实现类中重写该方法。
- default 方法能被覆写吗？→ 可以，实现类覆写后以实现类为准。

---

### 13.6 新时间日期 API (java.time)

**pdai 知识点回顾**：

> JDK 8 引入 `java.time` 包，替代了老旧的 `Date`/`Calendar`。核心类：`LocalDateTime`、`LocalDate`、`Instant`、`Duration`、`Period`、`DateTimeFormatter`。不可变、线程安全。

**新旧 API 对比**：

| 旧 API              | 新 API                         | 问题                |
| ------------------ | ----------------------------- | ----------------- |
| `Date`             | `Instant` / `LocalDateTime`   | Date 可变、月份从 0 开始  |
| `Calendar`         | `LocalDate` / `ZonedDateTime` | Calendar 可变、线程不安全 |
| `SimpleDateFormat` | `DateTimeFormatter`           | SDF 线程不安全（经典坑）    |
| `Date.getTime()`   | `Instant.toEpochMilli()`      | —                 |

**结合 SECP 业务场景**：

```java
// 全平台实体类统一使用 LocalDateTime（而非 Date）
// BlackListPO.java
private LocalDateTime createTime;     // 替代 Date
private LocalDateTime updateTime;

// WhiteListDao.java — MyBatis 参数直接传 LocalDateTime
void deleteList(@Param("blackIds") List<String> blackIds,
                @Param("operateIp") String operateIp,
                @Param("operateTime") LocalDateTime operateTime);

// RedisConfig.java — Duration 替代手工算毫秒
.readTimeout(Duration.ofMillis(timeout))    // Duration.ofMillis 替代 new Timeout(timeout)
.build();

// RedissonConfig.java — Duration 设置锁看门狗超时
.lockWatchdogTimeout(Duration.ofSeconds(10))  // 替代 10000L
```

**面试高频追问**：

| 问题                         | 回答                                                                                                                                          |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| SimpleDateFormat 线程安全问题    | SDF 内部用 Calendar，多线程共享会数据错乱。JDK 8 用 `DateTimeFormatter`（不可变、线程安全），或用 ThreadLocal 包装 SDF。                                                    |
| LocalDateTime 和 Date 互转    | `Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant())` / `Date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()` |
| Instant 和 LocalDateTime 区别 | Instant 是 UTC 时间戳（绝对时间），LocalDateTime 是本地时间（无时区信息）。                                                                                         |

---

### 13.7 其他新特性速答

| 特性                  | 说明                | SECP 场景                                      |
| ------------------- | ----------------- | -------------------------------------------- |
| `@Repeatable`       | 重复注解              | SECP 中 `@BusinessLock` 等自定义注解组合使用            |
| `CompletableFuture` | 异步编排              | 详见第二章 2.2 节                                  |
| `StampedLock`       | 乐观读锁              | SECP 未直接使用，Redisson 已满足分布式锁需求                |
| `Type Annotations`  | `@NonNull String` | 配合 Lombok `@NonNull` 做参数校验                   |
| `CompletableFuture` | 异步编程              | 结算单并行生成、批量审批（详见 2.2）                         |
| `String.join()`     | 字符串拼接             | `String.join(",", list)` 替代 StringBuilder 循环 |
| `Files.lines()`     | 文件流读取             | 边缘采集器日志处理                                    |

---

## 十四、安全认证与 OAuth2

> **pdai 知识体系**：安全认证是后端面试的高频话题。SECP 平台作为面向全球 100+ 国家的 SaaS 平台，有一套完整的认证授权体系：OAuth2 开放 API 授权 + JWT Token + Redis 会话管理 + Feign 令牌传递。

### 14.1 认证 vs 授权

**核心概念**：

| 概念  | 英文             | 回答                            |
| --- | -------------- | ----------------------------- |
| 认证  | Authentication | **你是谁？** 验证身份（用户名密码、Token、证书） |
| 授权  | Authorization  | **你能做什么？** 权限控制（RBAC、ABAC）    |
| 凭证  | Credential     | 证明身份的东西（密码、Token、OTP）         |
| 会话  | Session        | 认证后的状态保持                      |

> 简单记：认证 = 登录验证，授权 = 权限校验。SECP 中认证由 manager 模块负责，授权由 watchman 模块 + 数据权限缓存负责。

---

### 14.2 OAuth2 四种授权模式

**pdai 知识点回顾**：

> OAuth2 是一个授权框架，允许第三方应用在用户授权下访问用户在某服务上的资源，而不需要暴露用户密码。

**四种授权模式**：

| 模式                             | 适用场景         | 流程                         |
| ------------------------------ | ------------ | -------------------------- |
| **授权码模式** (authorization_code) | Web 应用（有服务端） | 用户跳转授权页→返回授权码→服务端用码换 Token |
| **客户端模式** (client_credentials) | 服务间调用（无用户参与） | 客户端 ID+密钥直接换 Token         |
| **密码模式** (password)            | 自有应用（信任度高）   | 用户名+密码直接换 Token            |
| **简化模式** (implicit)            | SPA 单页应用     | 授权页直接返回 Token（无授权码中间步）     |

**结合 SECP 业务场景**：

```java
// GrantTypesConstant.java — SECP 开放 API 网关定义了全部四种授权模式
public class GrantTypesConstant {
    public static final String AUTHORIZATION_CODE = "authorization_code";    // 授权码模式
    public static final String CLIENT_CREDENTIALS = "client_credentials";   // 客户端模式
    public static final String implicit = "implicit";                         // 简化模式
    public static final String PASSWORD = "password";                         // 密码模式
    public static final String FRESH_TOKEN = "refresh_token";                 // 刷新令牌
}

// OAuthController.java — 开放 API 的 Token 端点
@RestController
@RequestMapping("/oauth")
@Api(tags = "token接口")
public class OAuthController {
    @PostMapping("/token")
    @ApiOperation("获取accessToken")
    public Result<TokenResp> token(@Valid @RequestBody TokenReq req) {
        return ResultUtils.success(orderService.token(req));
    }
    // 免登 URL（授权码模式的简化跳转）
    @GetMapping("/token/url")
    @ApiOperation("获取免登url-GET")
    public void tokenUrlGet(TokenUrlReq req, HttpServletRequest request, ...) { ... }
}

// Oauth2ClientDetailsPO.java — OAuth2 客户端注册信息
public class Oauth2ClientDetailsPO {
    private String clientId;
    private String clientSecret;
    private String scope;                    // 授权范围
    private String authorizedGrantTypes;      // 允许的授权模式（逗号分隔）
}

// DaHuaFacadeServiceImpl.java — 对接大华监控平台，用客户端模式获取 Token
DaHuaAuthRequest daHuaAuthRequest = new DaHuaAuthRequest()
    .setGrantType("client_credentials")
    .setScope("server");
```

**面试加分回答**：

> "SECP 作为开放平台，为第三方接入提供了 OAuth2 授权。在开放 API 网关（middle-openapi-gw）中，我们定义了全部四种授权模式。最常用的是**客户端模式**——比如我们对接大华、海康监控平台时，第三方系统用 client_id + client_secret 直接换 access_token，不需要用户参与。而**授权码模式**用于 Web 应用接入——用户在 SECP 授权页面同意后返回授权码，第三方服务端用授权码换 Token，这是最安全的模式因为 Token 不经过浏览器。我们还有免登 URL 功能，让已登录用户自动跳转第三方系统，本质是授权码模式的优化体验。"

---

### 14.3 JWT 原理

**pdai 知识点回顾**：

> JWT (JSON Web Token) 是一种无状态的 Token 格式，由三部分组成：Header.Payload.Signature。

**JWT 结构**：

```
eyJhbGciOiJIUzI1NiJ9.     ← Header（Base64编码的JSON：算法类型）
eyJzdWIiOiJ1c2VyIn0.      ← Payload（Base64编码的JSON：用户信息、过期时间）
SflKxwRJSMeKKF2QT4f...     ← Signature（Header + Payload 用密钥签名）
```

**JWT vs Session 对比**：

| 特性  | JWT                    | Session            |
| --- | ---------------------- | ------------------ |
| 状态  | 无状态（服务端不存）             | 有状态（服务端存 Redis/内存） |
| 扩展性 | 天然支持水平扩展               | 需要共享 Session 存储    |
| 注销  | 困难（Token 未过期前一直有效）     | 简单（删 Redis key 即可） |
| 续期  | 困难（需重新签发）              | 简单（延长 Redis TTL）   |
| 安全  | Payload 可解码（不加密），签名防篡改 | 完全服务端控制            |

**结合 SECP 业务场景**：

> SECP **没有直接用 JWT**，而是用了 **Token + Redis 的有状态方案**。这是经过权衡的设计决策：

```java
// JwtProperties.java — JWT 配置（虽然叫 JWT，但实际是 Token+Redis 方案）
@ConfigurationProperties(prefix = "goodwe.security.jwt")
public class JwtProperties {
    private String header = "access-token";                // 请求头名称
    private Long tokenValidityInSeconds = 14400000L;       // 4 小时过期
    private String onlineKey = "online-token-";            // Redis key 前缀
    private Long detect = 3600000L;                        // 续期检查间隔（1 小时）
    private Long renew = 3600000L;                         // 续期时长（1 小时）
}
```

**面试标准回答**：

> "SECP 没有使用纯 JWT，而是采用了 Token + Redis 的有状态会话方案。原因是纯 JWT 有两个痛点：一是**主动注销困难**——JWT 签发后在过期前一直有效，用户退出登录后 Token 仍可用，这在企业级 SaaS 中不可接受；二是**续期困难**——JWT 需要重新签发，而 Token+Redis 方案只需延长 Redis TTL。SECP 的 `JwtProperties` 配置了 4 小时过期 + 1 小时检测 + 1 小时续期——用户在活跃使用时自动续期，空闲超过 1 小时检测到则不续，最终 4 小时后过期。这比 JWT 的'签发即不可控'灵活得多。"

---

### 14.4 Token + Redis 会话管理

**pdai 知识点回顾**：

> Token + Redis 方案：登录成功后生成随机 Token，以 Token 为 key、用户信息为 value 存入 Redis，设置过期时间。后续请求携带 Token，服务端从 Redis 查验。

**结合 SECP 业务场景**：

```java
// TokenRedisDao.java — SECP 的 Token 会话管理核心
@Component
public class TokenRedisDao {

    // === 存储 Session ===
    public void storeSession(UserPrinciple userPrinciple, String sessionId, boolean isMobile) {
        if (isMobile) {
            // 移动端：不设置过期时间（长期有效）
            securityRedisTemplate.opsForValue().set(
                getAppRedisKey(sessionId),
                JacksonUtils.getJsonConvert().toJsonString(userPrinciple));

            // Hash 结构记录用户所有登录会话：username → {sessionId__deviceCode: true}
            securityRedisTemplate.opsForHash().put(
                getAppKeysByUserName(userPrinciple.getUsername()),
                sessionId + "__" + userPrinciple.getMobileCode(),
                Boolean.TRUE.toString());
        } else {
            // Web 端：设置过期时间
            securityRedisTemplate.opsForValue().set(
                getWebRedisKey(sessionId),
                JacksonUtils.getJsonConvert().toJsonString(userPrinciple),
                jwtProperties.getTokenValidityInSeconds(),    // 4 小时 TTL
                TimeUnit.MILLISECONDS);
        }
    }

    // === 续期 ===
    public void setExpire(String sessionId, Long renew) {
        securityRedisTemplate.expire(getWebRedisKey(sessionId), renew, TimeUnit.MILLISECONDS);
    }

    // === 注销 ===
    public void destroyToken(String sessionId, boolean isMobile) {
        if (isMobile) {
            UserPrinciple user = getUserPrinciple(sessionId, true);
            if (user != null) {
                securityRedisTemplate.opsForHash().delete(
                    getAppKeysByUserName(user.getUsername()),
                    sessionId + "__" + user.getMobileCode());
            }
            securityRedisTemplate.delete(getAppRedisKey(sessionId));
        } else {
            securityRedisTemplate.delete(getWebRedisKey(sessionId));  // 删 Redis key = 注销
        }
    }

    // === Redis Key 设计 ===
    // Web:  goodwe:sebu:secp:sso:token:web:{sessionId}
    // App:  goodwe:sebu:secp:sso:token:app:{sessionId}
    // App 用户维度: goodwe:sebu:secp:sso:token:app:{username} → Hash{sessionId__deviceCode: true}
}
```

**SECP 的多端会话管理设计**：

```
Web 端（浏览器）：
  - Token 存 Redis，4 小时 TTL
  - 续期机制：每 1 小时检测，活跃则续 1 小时
  - 注销 = 删 Redis key

App 端（移动设备）：
  - Token 存 Redis，不设 TTL（长期有效）
  - 额外用 Hash 记录用户所有设备登录（支持多设备同时在线）
  - 注销 = 删 Token + 删 Hash 中的设备记录
  - 远程登出：把用户所有设备登录状态标记为 false
```

**面试加分回答**：

> "SECP 的会话管理区分 Web 和 App 两端。Web 端 Token 存 Redis 设 4 小时 TTL，配合检测+续期机制实现'活跃续期、空闲过期'。App 端 Token 不设过期时间（移动端用户体验要求），但用 Hash 结构记录用户名下所有设备登录——`username → {sessionId__deviceCode: true}`。这样管理员可以查到某用户所有登录设备，实现'远程登出'——把所有设备的登录标记设为 false，下次请求时检测到标记为 false 就拒绝访问。这个设计同时支持了多设备登录和远程注销，是 Token+Redis 方案的优势——比 JWT 灵活得多。"

---

### 14.5 Feign 令牌传递

**pdai 知识点回顾**：

> 微服务间调用时，下游服务也需要知道"是谁在调用"。Spring Cloud 中通过 Feign 拦截器在请求头中传递认证信息。

**结合 SECP 业务场景**：

```java
// SecurityInterceptor.java — SECP 的 Feign 令牌传递拦截器
@Component
public class SecurityInterceptor implements RequestInterceptor {

    public static final String TOKEN_APP_REDIS_KEY = "goodwe:sebu:secp:sso:token:app:";
    public static final String TOKEN_WEB_REDIS_KEY = "goodwe:sebu:secp:sso:token:web:";
    public static final String TOKEN_HEADER = "access-token";

    @Override
    public void apply(RequestTemplate requestTemplate) {
        // 1. 从 FeignContextHolder 获取认证信息（子线程场景）
        FeignAuthentication feignAuthentication = FeignContextHolder.getFeignAuthentication();
        String token = "";
        if (Objects.nonNull(feignAuthentication)) {
            token = feignAuthentication.getToken();
            // 传递 uuid、appVersion、pageKey 等上下文
        }

        // 2. 从 RequestContextHolder 获取当前 HTTP 请求（主线程场景）
        ServletRequestAttributes requestAttributes =
            (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (Objects.nonNull(requestAttributes)) {
            HttpServletRequest request = requestAttributes.getRequest();
            // 从 Cookie 或 Header 中获取 Token
            if (CharSequenceUtil.isBlank(getTokenByCookieOrHeader(request))) return;

            // 用 Token 查 Redis 获取用户信息
            UserPrincipleNoAuth userPrinciple = getUserUserPrinciple(token, appVersion);
            if (Objects.isNull(userPrinciple)) return;

            // 将用户信息放入 Feign 请求头
            requestTemplate.header(HttpUtils.TOKEN_HEADER, token);
            requestTemplate.header(HttpUtils.HEADER_USER_ID, userPrinciple.getId().toString());
            requestTemplate.header(HttpUtils.HEADER_USERNAME, userPrinciple.getUsername());
            requestTemplate.header(HttpUtils.HEADER_TENANT_ID, userPrinciple.getTenantId().toString());
        }
    }
}
```

**关键设计点**：

```java
// 注释原文：为什么不从RequestContextHolder拿信息？
// 因为单起的子线程里，每次feign请求之后都会触发RequestContextHolder.resetRequestAttributes()，
// 导致后续的请求会拿不到数据而报错
```

> 这段注释揭示了 SECP 解决**异步线程 Feign 令牌丢失**问题的方案：不依赖 `RequestContextHolder`（主线程才有），而是通过 `FeignContextHolder`（基于 ThreadLocal / TransmittableThreadLocal）在子线程中也能拿到认证信息。配合第二章的 `secp-context-carrier-agent`（Java Agent 字节码增强），实现跨线程令牌传递。

**面试标准回答**：

> "SECP 30+ 微服务之间用 OpenFeign 调用，认证信息通过 `SecurityInterceptor`（实现 `RequestInterceptor`）自动传递。拦截器从当前请求中提取 access-token，用 Token 查 Redis 获取用户信息（userId、username、tenantId），然后塞入 Feign 请求头传给下游。一个关键设计是处理异步线程——子线程中 `RequestContextHolder` 不可用（每次 Feign 调用后会 reset），所以我们用 `FeignContextHolder` 基于 ThreadLocal 单独存储认证信息，配合 Java Agent 字节码增强实现跨线程传递。"

---

### 14.6 SECP 安全架构全景

```
┌─────────────────────────────────────────────────────────────┐
│                      客户端（Web/App/第三方）                 │
│                   携带 access-token（Cookie/Header）          │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │   API 网关   │ ← 路由、限流、黑白名单
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │  认证中心（manager）      │
              │  - 登录/登出              │
              │  - Token 生成与校验       │
              │  - Token+Redis 会话管理   │
              │  - 多端会话/远程登出      │
              └────────────┬────────────┘
                           │ Feign + SecurityInterceptor
                    ┌──────▼──────┐
                    │  业务微服务   │ ← 请求头携带 userId/tenantId
                    │  (30+ 服务)  │
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │  权限校验（watchman）     │
              │  - 数据权限缓存（Redis）   │
              │  - UserThingPermissionCache│
              │  - RBAC 权限校验          │
              └─────────────────────────┘

开放 API 侧：
  第三方 → /oauth/token → OAuth2 授权 → access-token → API 调用
```

---

### 14.7 高频问答

| 问题                     | 回答要点                                                                 |
| ---------------------- | -------------------------------------------------------------------- |
| 为什么不用 Spring Security？ | SECP 自研轻量认证框架，Spring Security 太重、配置复杂。自研方案更灵活适配多端（Web/App/开放 API）。   |
| Token 怎么防重放？           | HTTPS 传输 + Token 有效期 + 可选 Nonce 机制。SECP 开放 API 侧还校验签名+时间戳。           |
| 多租户怎么隔离？               | Token 关联 tenantId，Feign 请求头传递，SQL 层面所有查询带 tenant_id 条件（见 6.1 节索引设计）。 |
| CSRF 怎么防？              | Token 放 Header（不放 Cookie）天然防 CSRF；如用 Cookie 则配合 SameSite 属性。         |
| XSS 怎么防？               | 前端转义 + CSP 策略 + HttpOnly Cookie。                                     |

---

## 十五、DevOps 与容器化

> **pdai 知识体系**：Docker、K8s、CI/CD、监控告警是现代后端开发的基础设施。SECP 30+ 微服务全部容器化部署在 K8s 上，配合 SkyWalking 链路追踪 + jemalloc 内存优化。

### 15.1 Docker 基础

**pdai 知识点回顾**：

> Docker 是轻量级容器技术，通过 Linux Namespace + Cgroups 实现进程隔离和资源限制。

**核心概念**：

| 概念             | 说明                        |
| -------------- | ------------------------- |
| 镜像 (Image)     | 只读模板，包含运行环境和应用代码          |
| 容器 (Container) | 镜像的运行实例                   |
| Dockerfile     | 构建镜像的指令文件                 |
| Registry       | 镜像仓库（Harbor / Docker Hub） |
| Namespace      | 隔离：PID、网络、挂载点、IPC、UTS、用户  |
| Cgroups        | 限制：CPU、内存、IO              |

**结合 SECP 业务场景**：

```dockerfile
# SECP 电费结算服务 Dockerfile（原文）
FROM 192.168.1.169:8281/common/openjdk-11-sw:v1.0    # 私有仓库基础镜像

WORKDIR /home
ENV TZ 'Asia/Shanghai'                                 # 时区
ENV LANG en_US.UTF-8

# 更换内存分配器：glibc → jemalloc，减少内存碎片
RUN yum install epel-release -y && yum install jemalloc -y
ENV LD_PRELOAD=/usr/lib64/libjemalloc.so.1

COPY secp-electricity-settlement-payment-service/target/*.jar /home

ENTRYPOINT java \
  -javaagent:/agent/skywalking-agent/skywalking-agent.jar \    # SkyWalking Agent
  -DSW_AGENT_COLLECTOR_BACKEND_SERVICES=skywalking-oap.skywalking.svc.cluster.local:11800 \
  -DSW_AGENT_NAME=secp-electricity-settlement-payment \        # 服务名
  -jar -server -Xms2g -Xmx2g \                                  # JVM 参数
  -XX:+HeapDumpOnOutOfMemoryError \                             # OOM 自动 dump
  -XX:HeapDumpPath=/home/error_data \
  *.jar --spring.profiles.active=$renv                          # 环境变量控制 profile
```

**SECP Dockerfile 设计要点**：

1. **统一基础镜像**：所有微服务用同一个 `openjdk-11-sw:v1.0`，内含 SkyWalking Agent
2. **环境隔离**：通过 `$renv` 构建参数控制 `spring.profiles.active`，一个镜像跑多环境
3. **jemalloc**：替换 glibc malloc 减少内存碎片（详见 15.4）
4. **OOM dump**：`HeapDumpOnOutOfMemoryError` + `HeapDumpPath` 自动 dump 便于排查

**面试高频追问**：

| 问题                                | 回答                                                                                                     |
| --------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Docker 和虚拟机的区别？                   | Docker 共享内核，毫秒级启动，资源占用小；虚拟机独立内核，分钟级启动，资源占用大。                                                           |
| 一个镜像怎么跑多环境？                       | 构建时传 `ARG renv`，运行时通过 `--spring.profiles.active=$renv` 选择配置。SECP 用 Nacos 配置中心，不同环境连不同 Nacos namespace。 |
| Dockerfile 的 CMD 和 ENTRYPOINT 区别？ | ENTRYPOINT 是容器启动命令（不易被覆盖），CMD 是默认参数（可被 docker run 覆盖）。SECP 用 ENTRYPOINT 确保启动命令不变。                      |

---

### 15.2 容器化 JVM 调优

**pdai 知识点回顾**：

> 容器环境下 JVM 需要感知容器资源限制。JDK 8u191+ 默认支持容器感知（`-XX:+UseContainerSupport`），但手动指定更可靠。

**SECP 容器 JVM 参数分析**：

```bash
# 从 Dockerfile 提取的关键 JVM 参数
-server                          # Server 模式（生产环境必选）
-Xms2g -Xmx2g                   # 堆固定 2G（避免扩容抖动）
-XX:+HeapDumpOnOutOfMemoryError  # OOM 自动 dump
-XX:HeapDumpPath=/home/error_data

# === 隐含的容器调优知识点 ===
# 1. 为什么 Xms = Xmx？
#    避免堆动态扩容导致的 GC 抖动。容器分配 4G 内存，JVM 堆 2G + 堆外 1G + JVM 自身开销 ≈ 3.5G，
#    留 500M 给容器 OS 进程，防止 OOM Killed。

# 2. 容器内为什么不用 G1？
#    SECP 部分服务仍用 JDK 11 默认 G1（JDK 9+ 默认），-Xmx2g 下 G1 表现良好。
#    关键服务（如 secp-algorithm）会在启动脚本中追加 G1 调优参数。

# 3. 为什么不用 -XX:+UseContainerSupport？
#    JDK 11 默认开启。但要注意：容器内存限制 ≠ 物理内存，JVM 默认按容器限制计算堆，
#    如果不指定 Xmx 可能占满容器内存。SECP 显式指定 Xmx=2g 更可控。
```

**容器 OOM Killed 排查**：

> 容器被 K8s OOM Killed ≠ JVM OOM。容器 OOM 是物理内存超限（堆 + 堆外 + Metaspace + 线程栈 + JNI + 容器进程）。排查步骤：
> 
> 1. `kubectl describe pod` 查看 `OOMKilled` 事件
> 2. `kubectl top pod` 看容器实际内存使用
> 3. 如果 JVM 堆没满但容器 OOM → 检查堆外内存（Netty ByteBuf、ThreadLocal 泄露、Metaspace）
> 4. SECP 的解决方案：jemalloc 替换 glibc malloc（减少碎片导致的虚高）+ `-XX:MaxDirectMemorySize=1g` 限制堆外

---

### 15.3 SkyWalking 链路追踪

**pdai 知识点回顾**：

> SkyWalking 是 APM（应用性能监控）系统，基于 Java Agent 字节码增强实现无侵入的链路追踪。核心概念：Trace（完整调用链）、Segment（单个服务的 span 集合）、Span（一次方法调用）。

**SkyWalking 架构**：

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Java Agent  │     │  Java Agent  │     │  Java Agent  │
│  (微服务 A)   │     │  (微服务 B)   │     │  (微服务 C)   │
│  - 字节码增强  │     │  - 字节码增强  │     │  - 字节码增强  │
│  - 自动注入   │     │  - 自动注入   │     │  - 自动注入   │
│  TraceId     │     │  TraceId     │     │  TraceId     │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │ gRPC                │ gRPC                │ gRPC
       └─────────────────────┼─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  OAP Server     │ ← 数据聚合、分析
                    │  (skywalking-oap)│
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Storage        │ ← ES / H2 / MySQL
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  UI Dashboard   │ ← 拓扑图、Trace 详情
                    └─────────────────┘
```

**结合 SECP 业务场景**：

```dockerfile
# 每个 SECP 微服务的 Dockerfile 都注入 SkyWalking Agent
ENTRYPOINT java \
  -javaagent:/agent/skywalking-agent/skywalking-agent.jar \           # ← Agent 挂载
  -DSW_AGENT_COLLECTOR_BACKEND_SERVICES=skywalking-oap.skywalking.svc.cluster.local:11800 \  # OAP 地址
  -DSW_AGENT_NAME=secp-electricity-settlement-payment \               # 服务注册名
  -jar *.jar
```

**SkyWalking 在 SECP 中的作用**：

1. **全链路追踪**：一个用户请求从网关 → manager → settlement → algorithm → data 的完整调用链，SkyWalking 自动串联 TraceId
2. **异步链路传递**：SECP 使用 `SupplierWrapper.of()` / `RunnableWrapper.of()` 包装 CompletableFuture 的任务，SkyWalking Agent 自动传递 TraceId 到异步线程
3. **性能分析**：SkyWalking UI 拓扑图展示各服务间的调用关系和耗时，快速定位慢节点
4. **告警**：配置响应时间阈值，超时自动告警到飞书

**面试标准回答**：

> "SECP 30+ 微服务全部通过 Dockerfile 中 `-javaagent:skywalking-agent.jar` 挂载 SkyWalking Agent，实现零侵入链路追踪。Agent 在类加载时用字节码增强拦截 HTTP、RPC、DB 等组件，自动生成 Span 并通过 gRPC 上报到 OAP Server。一个请求从网关到最终数据查询的完整链路在 SkyWalking UI 上一目了然。对于异步线程（CompletableFuture），我们用 SkyWalking 提供的 `SupplierWrapper` / `RunnableWrapper` 包装任务，保证 TraceId 跨线程传递。配合 Arthas 在线诊断，排查线上慢接口的流程是：SkyWalking 定位慢在哪个服务→哪个方法→Arthas trace 定位方法内哪行最慢。"

---

### 15.4 jemalloc 内存分配器

**结合 SECP 业务场景**：

```dockerfile
# SECP 结算服务 Dockerfile
# 更换内存分配器，将 glibc 换成jemalloc，减少内存碎片
RUN yum install epel-release -y && yum install jemalloc -y
ENV LD_PRELOAD=/usr/lib64/libjemalloc.so.1
```

**为什么换 jemalloc？**

| 问题     | glibc malloc    | jemalloc       |
| ------ | --------------- | -------------- |
| 内存碎片   | 高（碎片率可达 30%+）   | 低（分区管理）        |
| RSS 虚高 | 小对象释放后 RSS 不降   | 定期 purge 归还 OS |
| 多线程竞争  | 全局锁（后来改为 arena） | arena 分离，竞争更小  |
| 大对象    | 直接 mmap         | 按大小分级管理        |

> **面试场景**：Java 应用容器内存虚高，`kubectl top pod` 显示 3.5G 但 JVM 堆才 2G——多出来的 1.5G 很多时候是 glibc malloc 的碎片虚高。换 jemalloc 后 RSS 通常能降 20%-30%，避免容器被 OOM Killed。SECP 在所有微服务的 Dockerfile 中统一替换了 jemalloc。

---

### 15.5 高频问答

| 问题                      | 回答要点                                                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------------------------------- |
| Docker 镜像怎么分层？          | 每条 Dockerfile 指令一层，相同层复用缓存。SECP 基础镜像层（openjdk + SkyWalking Agent）被所有微服务复用。                                      |
| K8s 中 Pod 怎么做健康检查？      | Liveness Probe（存活探针）+ Readiness Probe（就绪探针）。SECP 配 Spring Boot Actuator 的 `/actuator/health` 端点。                |
| 容器里 jmap/jstack 能用吗？    | 能，但容器内 PID=1 是 Java 进程，直接 `jmap <pid>` 即可。或用 `arthas` attach。SECP 运维通过 `kubectl exec` 进入容器执行诊断命令。               |
| SkyWalking 和 Zipkin 区别？ | SkyWalking 用字节码增强（无侵入），Zipkin 需要手动埋点；SkyWalking 有服务拓扑图和告警，Zipkin 主要是 Trace 查看。SECP 选 SkyWalking 是因为零侵入 + 全链路拓扑。 |
| CI/CD 流程？               | GitLab CI → Maven 构建 → Docker build → 推送私有 Harbor → K8s kubectl apply。SECP 使用 GitLab Runner + 自研部署脚本。           |

---

## 十六、数据结构·树（结合 SECP 拓扑树 / 组织树 / 权限树）

> **pdai 知识点回顾**：树是面试中最高频的数据结构之一。Java 后端面试中考查的树包括：二叉树遍历、BST 性质、AVL/红黑树自平衡、B/B+ 树（数据库索引）、堆（PriorityQueue 底层）、Trie（前缀匹配）以及工程中大量使用的 N 叉树（组织树/权限树/菜单树）。

### 16.1 二叉树基础：遍历与性质

**面试题：二叉树有几种遍历方式？分别是什么？**

| 遍历方式                      | 顺序        | 典型用途                 |
| ------------------------- | --------- | -------------------- |
| **前序**（Pre-order）         | 根 → 左 → 右 | 拷贝树、序列化树结构           |
| **中序**（In-order）          | 左 → 根 → 右 | BST 中序遍历得到有序序列       |
| **后序**（Post-order）        | 左 → 右 → 根 | 删除树（先删子节点再删根）、计算目录大小 |
| **层序**（Level-order / BFS） | 逐层从上到下    | 按层级打印、找最近公共祖先        |

**核心性质速答**：

```
满二叉树：每层都满，第 k 层有 2^(k-1) 个节点，总节点数 2^k - 1
完全二叉树：只有最后一层可能不满，且从左到右连续
完全二叉树性质：节点数 n，则深度 = ⌊log₂n⌋ + 1
叶子节点数 = 度为 2 的节点数 + 1（n₀ = n₂ + 1）
```

**面试追问：递归遍历 vs 迭代遍历？**

```
递归：代码简洁，但递归深度受栈大小限制（默认 512KB，约 10000+ 层才溢出）
迭代：用栈模拟（前/中/后序）或队列模拟（层序），避免栈溢出
SECP 实践：拓扑树深度通常 ≤ 10 层（平台→租户→场站→系统→设备→节点），
  递归完全安全；权限树深度通常 ≤ 4 层（目录→菜单→按钮），更不是问题。
```

---

### 16.2 二叉搜索树 BST

**面试题：什么是 BST？为什么需要它？**

> BST（Binary Search Tree）：左子树所有节点 < 根 < 右子树所有节点，左右子树也是 BST。
> 
> - 查找/插入/删除：平均 O(log n)，最坏 O(n)（退化为链表）
> - 中序遍历 = 有序序列

**SECP 真实场景 — TreeMap 的 BST 本质**：

pdai 知识点：Java 的 `TreeMap` 底层就是红黑树（自平衡 BST）。SECP 中用 TreeMap 做第三方 API 签名时，本质就是利用 BST 的中序遍历有序性——`TreeMap.keySet()` 按 ASCII 字典序输出，无需额外排序。

```java
// SECP 签名代码（YongSignHelper）
TreeMap<String, String> params = new TreeMap<>();
params.put("appKey", "xxx");
params.put("timestamp", "1234567890");
params.put("nonce", "abc");
// TreeMap.keySet() 按 ASCII 排序 → 签名参数天然有序
```

**BST 退化问题**：

```
插入有序数据 [1,2,3,4,5] → 退化为链表，查找 O(n)
解决：自平衡 BST → AVL 树 / 红黑树
```

---

### 16.3 AVL 树 vs 红黑树

**面试高频题：AVL 树和红黑树有什么区别？为什么 HashMap 用红黑树不用 AVL？**

| 维度        | AVL 树                | 红黑树                                           |
| --------- | -------------------- | --------------------------------------------- |
| 平衡条件      | 严格平衡：任意节点左右子树高度差 ≤ 1 | 近似平衡：红黑约束保证最长路径 ≤ 2 × 最短路径                    |
| 查找性能      | 略快（更平衡，树更矮）          | 略慢                                            |
| 插入/删除     | 旋转次数多（可能多次旋转）        | 旋转次数少（最多 3 次旋转）                               |
| 适用场景      | 查找密集型（只读多、写少）        | 读写均衡                                          |
| Java 中的使用 | 几乎没有                 | **HashMap / TreeMap / ConcurrentHashMap** 桶转树 |

> **为什么 HashMap 选红黑树？** HashMap 桶链表转树后，频繁 put/remove，红黑树增删性能更好（旋转少），综合更优。AVL 严格平衡导致增删时旋转代价高。

**SECP 真实场景 — HashMap 链表转红黑树**：

```java
// JDK 8 HashMap 源码逻辑（SECP 中大量使用 HashMap 的地方都涉及）
// 链表长度 ≥ 8 且数组长度 ≥ 64 → 转红黑树
// 红黑树节点 ≤ 6 → 退化为链表
// 阈值 8 的原因：泊松分布下链表长度到 8 的概率约亿分之六
```

**红黑树五大性质速答**：

```
1. 每个节点是红色或黑色
2. 根节点是黑色
3. 叶子节点（NIL）是黑色
4. 红色节点的子节点必须是黑色（不能有连续红节点）
5. 从任意节点到其叶子节点的所有路径，包含相同数目的黑色节点（黑高相同）
```

---

### 16.4 B 树 / B+ 树（数据库索引核心）

> 此处不重复（详见 [6.1 索引原理](#61-索引原理b树与最左前缀)），仅做知识点串联。

**面试题：B+ 树相比 B 树的优势？**

| 维度       | B 树       | B+ 树                |
| -------- | --------- | ------------------- |
| 非叶子节点    | 存数据 + 索引  | 只存索引                |
| 单页能放多少键  | 少（数据占空间）  | 多（只放索引，16KB 页可放上千键） |
| 树的高度     | 较高        | 更矮（3 层可支撑千万级数据）     |
| 范围查询     | 需要中序遍历整棵树 | 叶子节点双向链表，顺序扫描即可     |
| 磁盘 IO 次数 | 多         | 少                   |

**SECP 真实场景**：

```sql
-- SECP 结算库 DDL（PostgreSQL），联合索引全部 tenant_id 打头
-- B+ 树最左前缀原则：查询条件必须从索引最左列开始
CREATE INDEX idx_settlement_tenant_station_month ON settlement_bill
    (tenant_id, station_id, settle_month);
-- ✅ SELECT ... WHERE tenant_id=? AND station_id=?   → 命中
-- ✅ SELECT ... WHERE tenant_id=?                     → 命中（最左前缀）
-- ❌ SELECT ... WHERE station_id=?                     → 不命中（跳过了 tenant_id）
```

**为什么不用红黑树做索引？** 红黑树是二叉树，千万级数据需要 23+ 层（2²³ ≈ 800 万），即 23 次磁盘 IO。B+ 树 3 层即可，只需 3 次 IO。

---

### 16.5 堆（Heap）与优先队列

**面试题：什么是堆？PriorityQueue 底层是什么？**

> 堆是一棵**完全二叉树**，用**数组**存储（不建 Node 对象），满足：
> 
> - **大顶堆**：每个节点 ≥ 子节点，根节点是最大值
> - **小顶堆**：每个节点 ≤ 子节点，根节点是最小值

```
数组索引关系（0 为根）：
  父节点索引 = (i - 1) / 2
  左子节点索引 = 2 * i + 1
  右子节点索引 = 2 * i + 2
```

**堆的操作复杂度**：

| 操作             | 时间复杂度    | 说明             |
| -------------- | -------- | -------------- |
| 插入（siftUp）     | O(log n) | 放末尾，向上调整       |
| 删除堆顶（siftDown） | O(log n) | 末尾替换堆顶，向下调整    |
| 建堆（heapify）    | O(n)     | 从最后一个非叶子节点开始下沉 |
| 取堆顶（peek）      | O(1)     | 数组第 0 个元素      |

**Java PriorityQueue**：

```java
// 默认小顶堆
PriorityQueue<Integer> minHeap = new PriorityQueue<>();
// 大顶堆
PriorityQueue<Integer> maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
```

**SECP 真实场景 — PriorityQueue 在告警优先级排序中的潜在应用**：

```
告警场景：SECP 的 Prophet 告警模块中，不同告警有不同严重级别
  （紧急 > 重要 > 次要 > 提示）。如果要做"取 Top N 最紧急告警"或
  "按优先级消费告警队列"，PriorityQueue / PriorityBlockingQueue 是自然选择。

实际代码中 Prophet 用的是 Redis ZSet 做跨 Pod 排序（分布式场景），
但如果单机内排序，PriorityQueue 是标准方案。
```

> **追问：堆排序 vs 快排？**
> 
> - 堆排序：时间 O(n log n)，空间 O(1)，不稳定，但缓存不友好（跳跃式访问数组）
> - 快排：平均 O(n log n)，最坏 O(n²)，缓存友好（顺序访问），不稳定
> - Java `Arrays.sort()` 基础类型用双轴快排，对象用 TimSort（归并+插入）

---

### 16.6 字典树 Trie

**面试题：什么是 Trie？什么场景用？**

> Trie（前缀树/字典树）：将字符串按字符拆分，共用前缀的字符串共享路径。根到某节点的路径 = 一个字符串。

```
           root
          / | \
         s  w  t
         |  |  |
         e  o  w (two)
         |  |  
         a  r  (word)
         |  
         r
         |
         h (search)
```

**Trie 操作复杂度**：

| 操作   | 时间复杂度      | 说明        |
| ---- | ---------- | --------- |
| 插入   | O(L)       | L = 字符串长度 |
| 查找   | O(L)       | 与树大小无关    |
| 前缀匹配 | O(L + 结果数) | 自动补全的核心   |

**Trie vs HashMap 做字符串查找？**

| 维度   | Trie                 | HashMap                   |
| ---- | -------------------- | ------------------------- |
| 查找   | O(L)，L=字符串长度         | O(L)（hash 计算） + O(1)（桶定位） |
| 前缀匹配 | O(L)，天然支持            | 不支持                       |
| 空间   | 共用前缀，省空间（大量短字符串时优势大） | 每个字符串独立存                  |
| 适用场景 | 自动补全、拼写检查、IP 路由      | 精确匹配                      |

**SECP 中的潜在场景**：

```
SECP 设备编码（thingCode）有层级前缀，如：
  SECP-PV-001  （光伏逆变器 001）
  SECP-PV-002
  SECP-BAT-001 （储能 001）
  SECP-METER-001

如果需要"输入 SECP-PV 自动补全所有 PV 设备编码"的功能，
Trie 是最优选择。实际代码中用 LIKE 查询或 Redis ZSet 做前缀匹配，
但面试时可以讲 Trie 作为算法层面的方案。
```

---

### 16.7 N 叉树：SECP 中的实际应用

> **这是 SECP 项目中用得最多的树结构**——组织树、权限树、拓扑树、菜单树全部是 N 叉树。

#### 16.7.1 拓扑树（TopologyTreeNode）

**SECP 真实代码**（`TopologyTreeNode.java`）：

```java
@Data
@Accessors(chain = true)
public class TopologyTreeNode {
    private Long nodeId;
    private Long parentNodeId;
    private NodeTypeEnum nodeType;           // GRID(并网) / LOAD(负荷) / GENERATE(发电) / STORAGE(储能)
    private String powerAccountNumber;       // 电力户号
    private List<TopologyTreeNode> childrenNodes;  // N 个子节点
}
```

> 这是一个典型的 **自引用 N 叉树节点**：`childrenNodes` 的类型就是 `List<TopologyTreeNode>`，形成无限递归的树结构。

**业务含义**：一个电站的电气拓扑树长这样：

```
电站根节点
├── 并网节点（Grid）— 关联电力户号
│   ├── 发电节点（Generate）
│   │   ├── 逆变器设备 1
│   │   └── 逆变器设备 2
│   └── 储能节点（Storage）
│       ├── 储能设备 1
│       └── 储能设备 2
└── 负荷节点（Load）
    ├── 负荷设备 1
    └── 负荷设备 2
```

#### 16.7.2 组织树（DeptInfo）

**SECP 真实代码**（`DeptInfo.java`）：

```java
@Data
public class DeptInfo {
    private Long id;
    private String name;
    private Long parentId;              // 父节点 ID（自引用）
    private Boolean related;            // 是否有人/物关联
    private Boolean allowDelete;        // 是否允许删除
    private Boolean allowModifyChildren; // 是否允许修改子节点
    private Boolean shareDept;          // 是否是资产共享挂载节点
    private Long tenantId;

    @Override
    public boolean equals(Object o) { return id.equals(((DeptInfo) o).id); }
    @Override
    public int hashCode() { return Objects.hash(id); }
}
```

> 注意：组织树在数据库中存的是**平铺的 parentId 引用**（邻接表模型），不是嵌套 JSON。前端展示时才组装成树。

#### 16.7.3 权限树（PermissionInfo）

**SECP 真实代码**（`PermissionInfo.java`）：

```java
@Data
public class PermissionInfo {
    private Long id;
    private String name;
    private Long parentId;              // 父权限 ID
    private Integer componentType;      // 0:菜单 1:链接 2:按钮 3:目录 4:分组
    private String permission;          // 权限标识：user:add
    private Boolean enable;             // 是否启用
    private Integer orderIdx;           // 排序字段
}
```

权限树的层级结构：

```
目录（componentType=3）
└── 菜单（componentType=0）
    ├── 按钮（componentType=2）— 如"创建用户"按钮
    ├── 按钮（componentType=2）— 如"删除用户"按钮
    └── 链接（componentType=1）
```

> **面试亮点**：权限树的 componentType 字段是一个枚举值，不同的值代表不同的 UI 组件类型。面试官问"你们权限系统怎么设计的"时，可以展开讲 RBAC + 树形权限 + 按钮级控制。

#### 16.7.4 N 叉树的存储模型对比

| 模型                | 存储           | 查子节点    | 查所有后代    | 查路径   | 适用场景         |
| ----------------- | ------------ | ------- | -------- | ----- | ------------ |
| **邻接表**（parentId） | 每行存 parentId | 1 次 SQL | 递归 / CTE | 递归    | SECP 组织树/权限树 |
| **路径枚举**（path）    | 存 `/1/2/3/`  | LIKE 查询 | LIKE 查询  | 拆分字符串 | 文件系统         |
| **嵌套集**（lft/rgt）  | 存左右值         | 范围查询    | 范围查询     | 范围查询  | 读多写少         |
| **闭包表**（关系表）      | 祖先-后代关系表     | JOIN    | JOIN     | JOIN  | 复杂查询         |

> SECP 选邻接表 + 递归/内存组装，因为组织树/权限树变更频率低，读时一次性查全量再在内存组装树。

---

### 16.8 树的遍历：BFS vs DFS 在 SECP 中的对比

> **这是面试中最容易结合项目讲出深度的部分**——SECP 代码中同时使用了 BFS 和 DFS 两种遍历方式。

#### 16.8.1 BFS（广度优先 / 层序遍历）

**SECP 真实代码 1 — 构建拓扑树**（`SniperFacadeServiceImpl`）：

```java
// 从平铺 List<TopologyNodeInfo> 构建树，用 BFS（Queue）遍历
Map<Long, TopologyNodeInfo> nodeIdInfoMap = 
    topologyNodeInfos.stream()
        .collect(Collectors.toMap(TopologyNodeInfo::getId, Function.identity()));

// 第 1 步：建立 parentId → childrenIds 的映射
Map<Long, List<Long>> parentNodeIdChildrenNodeIdsMap = new HashMap<>();
for (TopologyNodeInfo node : topologyNodeInfos) {
    if (!isRootNode(stationId, node)) {
        parentNodeIdChildrenNodeIdsMap
            .computeIfAbsent(node.getParentId(), k -> new ArrayList<>())
            .add(node.getId());
    }
}

// 第 2 步：BFS 从根节点开始，逐层构建 TreeNode 并挂载 children
Map<Long, TopologyTreeNode> nodeIdTreeNodeMap = new HashMap<>();
Queue<Long> queue = new LinkedList<>();
queue.add(rootNodeId);
while (!queue.isEmpty()) {
    Long currNodeId = queue.poll();
    TopologyTreeNode treeNode = convert(nodeIdInfoMap.get(currNodeId));
    nodeIdTreeNodeMap.put(currNodeId, treeNode);

    // 挂载到父节点
    Long parentId = treeNode.getParentNodeId();
    if (parentId != null && !currNodeId.equals(rootNodeId)) {
        nodeIdTreeNodeMap.get(parentId).getChildrenNodes().add(treeNode);
    }

    // 子节点入队 → 下一层
    List<Long> childrenIds = parentNodeIdChildrenNodeIdsMap.get(currNodeId);
    if (CollUtil.isNotEmpty(childrenIds)) {
        queue.addAll(childrenIds);
    }
}
```

> **为什么用 BFS 而不是 DFS？** BFS 保证了在处理子节点时，父节点一定已经创建并放入 Map。如果用 DFS，需要递归到叶子再返回，栈空间开销更大。BFS 用 Queue 迭代，没有栈溢出风险。

**SECP 真实代码 2 — 遍历拓扑树找并网节点**（`SettlementMeterServiceImpl`）：

```java
// BFS 遍历拓扑树，找所有 GRID 类型的节点
Queue<TopologyTreeNode> queue = new LinkedList<>();
queue.add(topologyTreeNode);  // 从根节点开始

while (!queue.isEmpty()) {
    TopologyTreeNode curr = queue.poll();
    if (!NodeTypeEnum.GRID.equals(curr.getNodeType())) {
        continue;  // 非并网节点，跳过但仍要遍历其子节点
    }
    // 处理并网节点 → 封装电力户号
    NodePowerAccountDTO dto = new NodePowerAccountDTO();
    dto.setNodeId(curr.getNodeId());
    dto.setPowerAccountNumber(curr.getPowerAccountNumber());
    nodePowerAccountDTOs.add(dto);

    // 子节点入队
    if (CollUtil.isNotEmpty(curr.getChildrenNodes())) {
        queue.addAll(curr.getChildrenNodes());
    }
}
```

#### 16.8.2 DFS（深度优先 / 递归）

**SECP 真实代码 3 — 递归删除权限树子节点**（`PermissionServiceImpl`）：

```java
// 禁用的权限要递归删除其所有子权限
@Override
public void removeDisabledPermissions(List<PermissionInfo> permissionInfoList) {
    Set<PermissionInfo> toRemove = new HashSet<>();
    for (PermissionInfo perm : permissionInfoList) {
        if (Boolean.FALSE.equals(perm.getEnable())) {
            toRemove.add(perm);
            removeChildren(perm.getId(), permissionInfoList, toRemove);  // 递归
        }
    }
    permissionInfoList.removeAll(toRemove);
}

private void removeChildren(Long parentId, List<PermissionInfo> list, Set<PermissionInfo> toRemove) {
    for (PermissionInfo perm : list) {
        if (perm.getParentId() != null && perm.getParentId().equals(parentId)) {
            toRemove.add(perm);
            removeChildren(perm.getId(), list, toRemove);  // 递归删除子节点
        }
    }
}
```

> **后序遍历思想**：先收集所有要删除的节点（包括子节点），最后统一 `removeAll`。这类似后序遍历——先处理子树，再处理根。

**SECP 真实代码 4 — DFS 搜索拓扑树找功率因数**（`MetricCompensationServiceImpl`）：

```java
// 在拓扑树中 DFS 搜索，找到第一个有功率因数值的节点
private Map<Long, Map<String, BigDecimal>> filterGridIdPfValueMap(
        Long gridNodeId, Map<Long, Map<String, BigDecimal>> nodeIdPfValueMap,
        Map<Long, TopologyNodeInfo> nodeIdInfoMap) {

    Map<String, BigDecimal> tagValueMap = nodeIdPfValueMap.get(gridNodeId);
    if (MapUtil.isNotEmpty(tagValueMap)) {
        BigDecimal pf = tagValueMap.get(BusinessDataTagEnum.PF.getTag());
        if (pf != null) {
            // 找到了！直接返回（DFS 的剪枝）
            Map<Long, Map<String, BigDecimal>> result = new HashMap<>();
            result.put(gridNodeId, tagValueMap);
            return result;
        }
    }

    // 当前节点没有 → DFS 往子节点找
    boolean hasChildren = false;
    for (Map.Entry<Long, TopologyNodeInfo> entry : nodeIdInfoMap.entrySet()) {
        if (Objects.equals(entry.getValue().getParentId(), gridNodeId)) {
            hasChildren = true;
            result.putAll(filterGridIdPfValueMap(entry.getKey(), nodeIdPfValueMap, nodeIdInfoMap));
        }
    }
    if (!hasChildren) {
        // 叶子节点且没有值 → 返回空
        resultMap.put(gridNodeId, Collections.emptyMap());
    }
    return resultMap;
}
```

> **DFS 剪枝**：找到第一个有值的节点就返回，不遍历整棵树。这是 DFS 相比 BFS 在"找第一个满足条件的节点"场景下的优势。

#### 16.8.3 BFS vs DFS 选型对比

| 维度          | BFS（层序/队列）        | DFS（递归/栈）     |
| ----------- | ----------------- | ------------- |
| 数据结构        | Queue             | Stack / 递归调用栈 |
| 空间          | O(w)，w = 最大宽度     | O(h)，h = 树高度  |
| 找最短路径       | ✅ 天然支持            | ❌ 需回溯         |
| 找第一个满足条件的节点 | 找到的不一定最优          | 可剪枝，提前终止      |
| 树很深         | 不会栈溢出             | 可能栈溢出         |
| 树很宽         | Queue 占内存大        | 空间省           |
| SECP 用法     | 构建树、层序遍历找 GRID 节点 | 递归删子节点、搜索功率因数 |

> **面试话术**：
> 
> "在 SECP 中，拓扑树的构建和遍历都用了 BFS——因为构建时要保证父节点先于子节点创建，用 Queue 天然满足这个顺序；遍历找并网节点时用 BFS 层序扫描。而权限树的递归删除子节点和拓扑树搜索功率因数用了 DFS——因为这两类场景要么是'删整棵子树'（后序思想），要么是'找到第一个就返回'（DFS 剪枝）。"

---

### 16.9 线段树与树状数组速答

**面试题：线段树和树状数组解决什么问题？**

| 数据结构          | 解决问题         | 时间复杂度    | 空间    |
| ------------- | ------------ | -------- | ----- |
| **线段树**       | 区间查询 + 区间修改  | O(log n) | O(4n) |
| **树状数组（BIT）** | 前缀和查询 + 单点修改 | O(log n) | O(n)  |

```
线段树：将区间 [1, n] 递归二分为子区间，每个节点维护一个区间信息
  查询 [l, r] 的和/最大值/最小值 → O(log n)
  修改某个位置的值 → O(log n)

树状数组（Fenwick Tree）：用 lowbit 巧妙管理前缀和
  lowbit(x) = x & (-x)  → x 在二进制中最低位的 1
  query(x) = 前 x 个元素的前缀和
  update(x, v) = 更新第 x 个位置

区别：线段树能做区间修改+区间查询，树状数组通常只做单点修改+前缀查询
```

> SECP 中没有直接使用线段树/树状数组（业务场景不需要高频区间统计），但面试时作为算法知识储备，了解原理即可。

---

### 16.10 高频问答

| 面试题                      | 答案要点                                                                                  |
| ------------------------ | ------------------------------------------------------------------------------------- |
| 二叉树前中后序遍历的区别？            | 根的位置：前序（根左右）、中序（左根右）、后序（左右根）。BST 中序遍历 = 有序序列                                          |
| 红黑树为什么比 AVL 更适合 HashMap？ | 红黑树近似平衡，增删旋转次数少（最多 3 次），综合性能优于严格平衡的 AVL                                               |
| B+ 树为什么适合做数据库索引？         | 非叶子节点只存索引 → 单页放更多键 → 树更矮 → IO 更少；叶子节点双向链表 → 范围查询高效                                    |
| 堆和 BST 的区别？              | 堆是完全二叉树+数组存储，只保证堆顶最大/小；BST 左<根<右，中序有序。堆适合 TopN/优先队列，BST 适合有序查找                        |
| N 叉树在项目中怎么存的？            | 邻接表模型：每行存 parentId，查询全量后在内存用 Map<parentId, List<child>> 组装树                           |
| 树的 BFS 和 DFS 各用什么数据结构？   | BFS 用 Queue，DFS 用 Stack 或递归调用栈。BFS 空间 O(宽度)，DFS 空间 O(深度)                              |
| 如何判断两棵树是否相同？             | 递归：两棵树根值相同 && 左子树相同 && 右子树相同。时间 O(min(n, m))                                          |
| 二叉树的最大深度？                | 递归：max(左子树深度, 右子树深度) + 1。也可以 BFS 层数。时间 O(n)                                           |
| 翻转二叉树？                   | 递归交换每个节点的左右子树。`TreeNode temp = root.left; root.left = root.right; root.right = temp;` |
| 最近公共祖先 LCA？              | 递归：如果当前节点等于 p 或 q，返回当前节点；否则递归左右子树，左右都非空则当前节点是 LCA                                     |

---

> **面试话术**：
> 
> "SECP 中最典型的树结构是电气拓扑树（TopologyTreeNode），它是一个自引用 N 叉树——每个节点有 nodeId、parentNodeId 和 List<TopologyTreeNode> childrenNodes。我们在构建这棵树时用 BFS（Queue）从根节点逐层构建，保证父节点先于子节点创建。遍历找并网节点时也用 BFS。而权限树的递归删除和拓扑树搜索功率因数用了 DFS，因为前者是后序思想（先删子再删根），后者需要剪枝（找到第一个就返回）。这些树结构在数据库里用的是邻接表模型（存 parentId），查出来在内存组装。"

---

## 十七、综合面试场景题

### 场景一：千万级设备数据采集如何保证不丢数据？

**面试官问**：你们平台千万级设备数据上报，怎么保证不丢数据？涉及哪些 Java 基础知识？

**回答框架**：

> "SECP 设备数据全链路：MQTT → Kafka → Flink → ES/ClickHouse。这条链路涉及 Java 集合、多线程、IO、JVM 四大基础：
> 
> **集合层**：Kafka 消费者用 `ConcurrentHashMap` 维护分区偏移量缓存，多消费者线程并发读写偏移量。消费到的消息批量写入 `ArrayList<Message>` 缓冲，达到 batch.size 或 batch.time 后 flush 到下游。
> 
> **多线程层**：MQTT 消费端用 Netty 的 EventLoop(NIO Reactor 模型)，一个线程管理数千设备连接。Kafka 消费者线程数 = 分区数，每个分区一个消费线程。Flink 用 `ThreadPoolExecutor` 管理 Source/Sink 并行度。
> 
> **IO 层**：MQTT(Netty NIO) 避免一连接一线程。Kafka 消费用零拷贝 `FileChannel.transferTo()` 从 Broker 拉数据。ES 写入用 Bulk API 批量减少 HTTP 请求次数。
> 
> **JVM 层**：G1 回收器配置 -XX:MaxGCPauseMillis=200 保证 GC 停顿不超 200ms，防止消费线程因 STW 导致 Kafka 心跳超时被踢出消费组。堆外内存(-XX:MaxDirectMemorySize=1g)限制 Netty 的 ByteBuf 使用量，防止 Direct Memory OOM。
> 
> **幂等保障**：Kafka 消费者设置 `enable.auto.commit=false`，手动同步提交偏移量。下游 ES 写入用 `doc_as_upsert` 实现幂等写入，重复消费不会产生重复数据。"

---

### 场景二：电费结算幂等性怎么保证？

**面试官问**：你说电费结算准确率 100%，怎么做到的？

**回答框架**：

> "结算幂等性涉及三层保障：
> 
> **1. 分布式锁(Redisson)**：定时任务每天凌晨跑批生成结算单，手动也可能触发。通过 `@BusinessLock` 注解 + AOP 切面 + Redisson RReadWriteLock 保证同一协议+月份只有一个线程在生成结算单。底层是 Redis + Lua 脚本 + Hash 结构(可重入) + 看门狗(续期)。
> 
> **2. RocketMQ 事务消息**：结算单生成后发 RocketMQ 事务消息到对账消费者，事务消息保证本地事务(写结算单)和消息发送的原子性——要么都成功，要么都失败。
> 
> **3. 数据库唯一约束**：结算单表对(协议ID, 结算月份)建唯一索引，即使分布式锁失效也能通过数据库约束兜底。
> 
> **4. 状态机控制**：结算单状态机(草稿→待审批→审批中→已审批→已出账单→已支付)用 AtomicInteger 或数据库乐观锁(version 字段)保证状态流转的原子性。"

---

### 场景三：30+ 微服务的线程池怎么管理？

**面试官问**：你们 30+ 微服务，线程池是怎么配置和管理的？

**回答框架**：

> "**隔离原则**：不同业务不同线程池，避免相互影响。在 SECP 电费结算模块中配置了 6 个独立线程池：
> 
> | 线程池                      | 核心/最大 | 队列  | 用途       |
> | ------------------------ | ----- | --- | -------- |
> | writeOssExecutor         | 10/20 | 200 | 写 OSS 文件 |
> | getFileUrlExecutor       | 10/10 | 200 | 获取文件 URL |
> | noticeSettlementExecutor | 10/10 | 200 | 结算通知     |
> | syncYongYouExecutor      | 10/10 | 200 | 用友开票同步   |
> | weaverExecutor           | 10/20 | 200 | 泛微对接     |
> | billInvoiceTimeExecutor  | 10/20 | 200 | 账单发票定时   |
> 
> **拒绝策略**：全部用 CallerRunsPolicy——队列满时由调用线程自己执行，相当于背压限流，保证任务不丢。
> 
> **上下文传递**：每个线程池配置 `MdcTaskDecorator` 传递 MDC 日志上下文 + SkyWalking 的 `SupplierWrapper`/`RunnableWrapper` 包装任务传递 TraceId。
> 
> **监控**：Prometheus 采集线程池指标(activeCount, queueSize, completedTaskCount)，Grafana 看板展示，队列堆积超阈值告警到飞书。
> 
> **为什么不用 Executors.newCachedThreadPool？** 因为它没有队列上限(maximumPoolSize=Integer.MAX_VALUE)，高并发下可能创建大量线程导致 OOM。我们手动配置 ThreadPoolExecutor，明确控制核心线程数、最大线程数和队列容量。"

---

### 场景四：如何排查线上接口响应慢的问题？

**面试官问**：场站概览页接口从 1.5s 降到 200ms，你怎么排查和优化的？

**回答框架**：

> "**排查工具链**：
> 
> 1. SkyWalking 链路追踪 → 发现慢在哪个微服务、哪个方法
> 2. Arthas `trace` 命令 → 定位方法内部哪行最耗时
> 3. `jstack` → 查看线程状态，是否有 BLOCKED/WAITING
> 4. 慢 SQL 日志 → 数据库层面排查
> 
> **发现的问题**：
> 
> 1. 场站列表查询关联了 5 张表，其中有 2 张大表(设备表百万级)没有走索引
> 2. 每次请求都查询数据库获取场站信息，没有缓存
> 3. 串行调用 3 个下游微服务(OpenFeign)，每次 HTTP 往返耗时
> 
> **优化方案**：
> 
> 1. **SQL 优化**：加索引 + 拆分子查询 + 用 ClickHouse 替代 PostgreSQL 做聚合统计
> 2. **Redis 多级缓存**：场站基础信息存 Redis(TTL 30 分钟)，用户级缓存(用户只能看到自己权限内的场站)
> 3. **并行调用**：3 个下游 Feign 调用改为 `CompletableFuture.allOf()` 并行，总耗时 = max(3 个调用) 而非 sum
> 4. **JVM 调优**：G1 回收器 + -XX:MaxGCPauseMillis=200，减少 STW 对接口延迟的影响
> 
> **结果**：1.5s → 200ms，QPS 提升 5 倍。"

---

### 场景五：几十亿条设备 5 分钟功率数据，怎么存、怎么查？

**面试官问**：你们平台几十万台设备每 5 分钟上报一次数据，历史数据几十亿条，数据库怎么设计的？为什么不用 MySQL 分库分表？

**回答框架**：

> "**量级估算**：30 万设备 × 288 点/天 ≈ 8600 万条/天，一年 300 亿+，纯插入 + 按时间范围查询场景。
> 
> **选型：PostgreSQL + TimescaleDB**：
> 
> 1. 业务表建好后一条 `create_hypertable('station_pv_power_5min_t0', 'date_time', chunk_time_interval => INTERVAL '1 day')` 转成超表
> 2. 底层自动按天分区（chunk），对应用透明——**写入永远落在最新 chunk**，索引小、全在热内存
> 3. 查询自动**分区裁剪**：查最近 7 天只扫 7 个 chunk，不碰历史数据
> 4. 历史数据保留策略：直接 **DROP 整个 chunk**，秒级删除、无碎片，比 DELETE 快几个量级
> 5. 冷数据可用 TimescaleDB 原生**列存压缩**，压缩比可达 90%+
> 
> **主键设计 `(station_id, date_time)`**：等值 + 范围查询完全命中，同时天然防重（幂等写入）。
> 
> **为什么不用 MySQL 分库分表**：分库分表（ShardingSphere）需要应用层路由、跨分片聚合复杂、扩容要迁移数据；TimescaleDB 用数据库原生分区换掉这套复杂度，单库即可承载，开发和运维成本都更低。**技术选型本质是业务驱动**——我们的场景是时序写入，不是高并发 OLTP 交易。
> 
> **配套措施**：数据经 Kafka → 消费批量 upsert 写入（ON CONFLICT DO UPDATE 幂等）；实时大屏走 Redis 缓存最新值，历史曲线查 TimescaleDB，聚合报表走 ClickHouse。"

---

## 附：pdai.tech 知识体系对应链接

| 模块              | pdai.tech 链接                                                                     |
| --------------- | -------------------------------------------------------------------------------- |
| Java 集合         | <https://www.pdai.tech/md/java/collections/java-collections-overview.html>       |
| JUC 并发框架        | <https://www.pdai.tech/md/java/thread/java-thread-x-juc-overview.html>           |
| synchronized    | <https://www.pdai.tech/md/java/thread/java-thread-x-key-synchronized.html>       |
| volatile        | <https://www.pdai.tech/md/java/thread/java-thread-x-key-volatile.html>           |
| CAS 与原子类        | <https://www.pdai.tech/md/java/thread/java-thread-x-juc-CAS.html>                |
| AQS             | <https://www.pdai.tech/md/java/thread/java-thread-x-juc-tool-aqs.html>           |
| 线程池             | <https://www.pdai.tech/md/java/thread/java-thread-x-juc-executor.html>           |
| Java IO         | <https://www.pdai.tech/md/java/io/java-io-overview.html>                         |
| BIO/NIO/AIO     | <https://www.pdai.tech/md/java/io/java-io-bio.html>                              |
| 零拷贝             | <https://www.pdai.tech/md/java/io/java-io-nio-zerocopy.html>                     |
| JVM 内存结构        | <https://www.pdai.tech/md/java/jvm/java-jvm-struct.html>                         |
| 类加载机制           | <https://www.pdai.tech/md/java/jvm/java-jvm-classload.html>                      |
| 垃圾回收            | <https://www.pdai.tech/md/java/jvm/java-jvm-gc.html>                             |
| G1 回收器          | <https://www.pdai.tech/md/java/jvm/java-jvm-gc-g1.html>                          |
| JMM             | <https://www.pdai.tech/md/java/jvm/java-jvm-jmm.html>                            |
| JVM 调优参数        | <https://www.pdai.tech/md/java/jvm/java-jvm-param.html>                          |
| Arthas          | <https://www.pdai.tech/md/java/jvm/java-jvm-agent-arthas.html>                   |
| 设计模式总览          | <https://www.pdai.tech/md/java/basic/java-basic-x-design.html>                   |
| 单例模式            | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-singleton.html> |
| 工厂模式            | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-factory.html>   |
| 策略模式            | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-strategy.html>  |
| 模板方法模式          | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-template.html>  |
| 观察者模式           | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-observer.html>  |
| 责任链模式           | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-chain.html>     |
| 代理模式            | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-proxy.html>     |
| 外观模式            | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-facade.html>    |
| 适配器模式           | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-adapter.html>   |
| 建造者模式           | <https://www.pdai.tech/md/java/basic/java-basic-x-design-pattern-builder.html>   |
| SQL 必备          | <https://www.pdai.tech/md/sql/sql-db-mysql.html>                                 |
| MySQL 索引/锁/事务   | <https://www.pdai.tech/md/sql/sql-db-mysql-theory.html>                          |
| MySQL 高频面试题     | <https://www.pdai.tech/md/sql/sql-db-mysql-interview.html>                       |
| SQL 优化          | <https://www.pdai.tech/md/sql/sql-db-mysql-optimization.html>                    |
| Redis 知识体系      | <https://www.pdai.tech/md/db/db-redis-overview.html>                             |
| Redis 高频面试题     | <https://www.pdai.tech/md/db/db-redis-x-questions.html>                          |
| Redis 数据结构      | <https://www.pdai.tech/md/db/db-redis-data-types.html>                           |
| Redis 持久化       | <https://www.pdai.tech/md/db/db-redis-x-rdb-aof.html>                            |
| Redis 高可用       | <https://www.pdai.tech/md/db/db-redis-x-sentinel.html>                           |
| Redis 集群        | <https://www.pdai.tech/md/db/db-redis-x-cluster.html>                            |
| 分布式锁            | <https://www.pdai.tech/md/db/db-redis-x-lock.html>                               |
| Kafka 知识体系      | <https://www.pdai.tech/md/db/db-kafka/db-kafka-overview.html>                    |
| Kafka 高频面试题     | <https://www.pdai.tech/md/db/db-kafka/db-kafka-questions.html>                   |
| RocketMQ 知识体系   | <https://www.pdai.tech/md/db/db-rocketmq/db-rocketmq-overview.html>              |
| RocketMQ 高频面试题  | <https://www.pdai.tech/md/db/db-rocketmq/db-rocketmq-questions.html>             |
| Spring AOP 详解   | <https://www.pdai.tech/md/spring/spring-x-framework-aop.html>                    |
| SpringBoot 知识体系 | <https://www.pdai.tech/md/spring/springboot/springboot.html>                     |
| Java 8 新特性      | <https://www.pdai.tech/md/java/java-x-newapi.html>                               |
| 数据结构与算法总览       | <https://www.pdai.tech/md/algorithm/algorithm.html>                              |
| 树结构基础           | <https://www.pdai.tech/md/algorithm/dev-basic-algorithm-tree.html>               |
| Stream API      | <https://www.pdai.tech/md/java/java-x-newapi-stream.html>                        |
| Lambda 表达式      | <https://www.pdai.tech/md/java/java-x-newapi-lambda.html>                        |
| Optional        | <https://www.pdai.tech/md/java/java-x-newapi-optional.html>                      |
| Docker 知识体系     | <https://www.pdai.tech/md/devops/docker/docker-01-overview.html>                 |
| Docker 核心原理     | <https://www.pdai.tech/md/devops/docker/docker-02-core.html>                     |
| K8s 知识体系        | <https://www.pdai.tech/md/devops/k8s/k8s-01-overview.html>                       |
| 全栈知识大纲          | <https://www.pdai.tech/md/outline/x-outline.html>                                |

---

> **面试核心原则**：每个 Java 基础知识点都要能映射到 SECP 平台的真实业务场景和代码实现，面试官问"你理解 HashMap 吗"时，不只是背八股，而是"在 SECP 结算模块中我这样用 HashMap..."。这才是有竞争力的面试回答。
