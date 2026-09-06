<p align="center">
  <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://github.com/qdrant/qdrant/raw/master/docs/logo-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://github.com/qdrant/qdrant/raw/master/docs/logo-light.svg">
      <img height="100" alt="Qdrant" src="https://github.com/qdrant/qdrant/raw/master/docs/logo.svg">
  </picture>
</p>

<p align="center">
    <b>面向下一代 AI 应用程序的向量搜索引擎</b>
</p>

<p align="center">
    <a href="README.md">English</a> · <b>简体中文</b>
</p>

<p align=center>
    <a href="https://github.com/qdrant/qdrant/actions/workflows/rust.yml"><img src="https://img.shields.io/github/actions/workflow/status/qdrant/qdrant/rust.yml?style=flat-square" alt="测试状态"></a>
    <a href="https://api.qdrant.tech/"><img src="https://img.shields.io/badge/Docs-OpenAPI%203.0-success?style=flat-square" alt="OpenAPI 文档"></a>
    <a href="https://github.com/qdrant/qdrant/blob/master/LICENSE"><img src="https://img.shields.io/github/license/qdrant/qdrant?style=flat-square" alt="Apache 2.0 许可证"></a>
    <a href="https://qdrant.to/discord"><img src="https://img.shields.io/discord/907569970500743200?logo=Discord&style=flat-square&color=7289da" alt="Discord 社区"></a>
    <a href="https://qdrant.to/roadmap"><img src="https://img.shields.io/badge/Roadmap-2025-bc1439.svg?style=flat-square" alt="2025 路线图"></a>
    <a href="https://cloud.qdrant.io/"><img src="https://img.shields.io/badge/Qdrant-Cloud-24386C.svg?logo=cloud&style=flat-square" alt="Qdrant 云服务"></a>
</p>

**Qdrant**（发音为 *quadrant*）是一款向量相似度搜索引擎与向量数据库。
它提供生产级服务和便捷的 API，用于存储、检索与管理数据点（Points——包含附加载荷 Payload 的向量）。
Qdrant 专门针对强大的过滤扩展能力进行了优化，非常适用于各类基于神经网络或语义的匹配、分面检索以及其他 AI 应用场景。

Qdrant 采用 Rust 🦀 编写，即使在高并发高负载下也能保持极高的性能与可靠性。详见[性能基准测试](https://qdrant.tech/benchmarks/)。

借助 Qdrant，嵌入向量（Embeddings）或神经网络编码器可以轻松构建为成熟的应用程序，用于匹配、搜索、推荐等诸多场景！

Qdrant 还提供全托管的 **[Qdrant Cloud](https://cloud.qdrant.io/)** ⛅ 云服务，包含**免费套餐**。

<p align="center">
<strong><a href="https://qdrant.tech/documentation/quickstart/">快速上手</a> • <a href="#agent-skills">Agent Skills</a> • <a href="#客户端库">客户端库</a> • <a href="#演示项目">演示项目</a> • <a href="#生态集成">生态集成</a> • <a href="#联系方式">联系方式</a>

</strong>
</p>

## 快速上手

### Agent Skills

Qdrant 提供了一组开箱即用的 [Agent Skills](https://github.com/qdrant/skills)，可将 Qdrant 的向量检索能力直接注入到你的 AI 编码助手当中。安装这些 Skills 可让你的 Agent 在面对向量检索的核心工程决策时（如量化策略、分片规划、多租户隔离、混合检索、模型迁移等）提供专业指导与最佳实践。

### 客户端-服务器模式 (Client-Server)

如需在本地完整体验 Qdrant 的强大功能，请使用以下命令启动容器：

```bash
docker run -p 6333:6333 qdrant/qdrant
```

请注意，该命令启动的是未开启认证的不安全部署，并向所有网络接口开放。详情请参阅[保护你的实例安全](https://qdrant.tech/documentation/security/#secure-your-instance)。

现在你可以使用任意[客户端](#客户端库)连接到服务器。例如使用 Python：

```python
from qdrant_client import QdrantClient

client = QdrantClient(url="http://localhost:6333")
```

在将 Qdrant 部署到生产环境之前，请务必阅读我们的[安装指南](https://qdrant.tech/documentation/installation/)与[安全指南](https://qdrant.tech/documentation/security/)。

### 客户端库

Qdrant 提供以下官方及社区客户端库，助你轻松集成到应用程序技术栈中：

- 官方客户端：
  - [Go 客户端](https://github.com/qdrant/go-client)
  - [Rust 客户端](https://github.com/qdrant/rust-client)
  - [JavaScript/TypeScript 客户端](https://github.com/qdrant/qdrant-js)
  - [Python 客户端](https://github.com/qdrant/qdrant-client)
  - [.NET/C# 客户端](https://github.com/qdrant/qdrant-dotnet)
  - [Java 客户端](https://github.com/qdrant/java-client)
- 社区客户端：
  - [Kotlin](https://github.com/NaCode-Studios/Kdrant)
  - [PHP](https://github.com/hkulekci/qdrant-php)

### Qdrant Edge

[Qdrant Edge](https://qdrant.tech/documentation/edge/) 是专为边缘设备和资源受限环境打造的轻量级 Qdrant 版本。与采用客户端-服务器架构的 Qdrant Server 不同，Qdrant Edge 直接运行在应用程序进程内。数据在本地存储与查询，并可与远程 Qdrant 服务器保持同步。它提供与服务端版本相同强大的向量检索能力，但资源占用更少，非常适合需要低延迟与离线可用性的应用场景。

若要在 Python 或 Rust 中使用 Qdrant Edge，请初始化 `EdgeShard` 实例，该实例提供了管理数据、执行查询和恢复快照的方法。示例如下：

```python
from qdrant_edge import Distance, EdgeConfig, EdgeVectorParams, EdgeShard, Point, UpdateOperation

shard = EdgeShard.create("./shard", EdgeConfig(
    vectors={"my-vector": EdgeVectorParams(size=4, distance=Distance.Cosine)}
))
shard.update(UpdateOperation.upsert_points([
    Point(id=1, vector={"my-vector": [0.1, 0.2, 0.3, 0.4]}, payload={"color": "red"})
]))
```

### 更多学习资源

- [快速入门指南](https://qdrant.tech/documentation/quickstart/)
- 详细[官方文档](https://qdrant.tech/documentation/)
- 参加 [Qdrant Essentials 入门课程](https://qdrant.tech/course/essentials/)
- 按照[本教程](https://qdrant.tech/documentation/tutorials-basics/search-beginners/)使用 Qdrant 构建语义搜索引擎

## 演示项目

### 探索语义文本搜索 🔍

释放语义嵌入向量的威力，超越传统的关键词检索，在短文本中发现深层语义关联。使用预训练神经网络仅需数分钟即可部署神经搜索，体验新一代文本检索技术。[在线体验！](https://qdrant.to/semantic-search-demo)

### 体验相似图像搜索 - 美食探索 🍕

探索的形态不止于文本搜索，尤其在美食领域。人们往往根据食物的外观而非文字描述或配料表来选择餐品。让 Qdrant 借助以图搜图技术，帮助用户发现下一道美味佳肴，即使他们不知道菜品名称也能轻松找到。[立即体验！](https://qdrant.to/food-discovery)

### 掌握极端分类 - 电商产品分类 📺

迈入极端分类（Extreme Classification）的前沿领域——这是一个应对拥有数百万类别与多标签难题的新兴机器学习方向。发掘相似度学习模型的巨大潜力，了解预训练 Transformer 模型与 Qdrant 如何革新电商产品自动化分类流程。[在线试用！](https://qdrant.to/extreme-classification-demo)

## API

### REST

Qdrant 提供了符合 [OpenAPI 3.0 规范](https://api.qdrant.tech/)的 REST API，支持为几乎所有编程语言和框架自动生成客户端代码。

你也可以直接下载原始 [OpenAPI 定义文件](https://github.com/qdrant/qdrant/blob/master/docs/redoc/master/openapi.json)。

### gRPC

对于要求更高吞吐与更低延迟的生产级搜索场景，Qdrant 还提供了高效的 [gRPC 接口](https://qdrant.tech/documentation/interfaces/#grpc-interface)。

## 特性

### 稠密、稀疏与多向量检索

Qdrant 全面支持用于语义相似度的稠密向量（Dense Vectors）、用于全文检索的稀疏向量（Sparse Vectors），以及针对具有多嵌入表示的对象或后期交互模型（如 ColBERT）的多向量检索（Multi-vector Search）。

### 基于载荷（Payload）的高级过滤

为向量附加任意 JSON 载荷，并通过丰富的条件表达式进行精确过滤——涵盖关键字匹配、全文检索、数值范围、地理位置等多种条件，并可使用 `should`、`must` 和 `must_not` 子句自由组合。

### 混合检索 (Hybrid Search)

在单次查询中融合多种向量类型，兼顾深度语义理解与精确关键字匹配；结果通过可配置的融合策略（如倒数排名融合 RRF 和基于分布的分数融合 DBSF）进行高效合并。

### 向量量化与磁盘存储

内置量化功能最高可减少 97% 的内存（RAM）占用，并允许在检索速度与精度之间灵活平衡。

### 分布式部署

通过分片（Sharding）与副本（Replication）实现水平扩展，并可在零停机时间内更新或调整集合容量。

### 核心亮点特性

* **分面聚合 (Faceting)** - 按载荷字段值对检索结果进行聚合统计。
* **相似推荐 (Recommendation)** - 利用正例和负例寻找相似的数据点。
* **空间探索 (Discovery)** - 将检索范围约束在向量空间的指定区域内。
* **检索相关性微调 (Search Relevance Tuning)** - 提供微调搜索结果的算法工具，例如最大边界相关算法（MMR）与相关性反馈查询（Relevance Feedback Query）。
* **多租户支持 (Multitenancy)** - 为多用户生产环境提供可横向扩展的数据分区机制。
* **可观测性 (Observability)** - 提供完善的指标监控（Metrics）、遥测（Telemetry）与审计日志，便于系统监控和排错。
* **查询规划与载荷索引 (Query Planning and Payload Indexes)** - 充分利用存储的载荷元数据信息智能优化查询执行计划。
* **SIMD 硬件加速** - 深度适配现代 x86-64 CPU 与 ARM Neon 架构指令集以提供极致计算性能。
* **GPU 加速支持** - 支持 NVIDIA 与 AMD GPU，大幅加速向量索引构建。
* **异步 I/O (Async I/O)** - 基于 `io_uring` 提升磁盘 I/O 吞吐；生产环境应优先使用本地 NVMe/SSD，并避免使用网络挂载存储（NAS）。
* **预写日志 (Write-Ahead Logging / WAL)** - 确保持久化存储与写入确认，即使遭遇断电或异常崩溃也能保障数据完整。

### Web 控制台 (Web UI)

Web 控制台为交互数据和监控部署健康状态提供了直观的可视化界面。支持浏览集合（Collections）、管理数据、直接与 REST API 交互等。

<p align="center"><img style="width: 75%; border: 1px solid #8f98b2;" src="https://qdrant.tech/docs/gettingstarted/web-ui.png" alt="Qdrant Web UI" /></p>

## 生态集成

Qdrant 可与现代 AI 技术栈各个阶段所使用的工具无缝集成。你可以轻松连接主流嵌入模型供应商、AI 应用开发框架、数据管道工具，以及用于在生产环境中监控和追踪向量检索的可观测性平台；同时也支持主流无代码和低代码自动化平台。完整集成列表请参阅[生态系统页面](https://qdrant.tech/documentation/ecosystem/)。

## 参与贡献

我们非常欢迎来自社区的贡献！在提交 Pull Request 之前，请先阅读我们的[贡献指南](docs/CONTRIBUTING.md)。

> [!IMPORTANT]
> 我们的主开发分支是 `dev`，而非 `master`。请在 Fork 仓库后基于 `dev` 分支创建工作分支，并将 Pull Request 目标分支指向 `dev`。指向 `master` 的 PR 将会被要求更改目标分支。

## 联系方式

- 有任何疑问？欢迎加入我们的 [Discord 频道](https://qdrant.to/discord) 或在 X 上 [@qdrant_engine](https://qdrant.to/twitter)
- 想了解最新发布动态？欢迎订阅我们的[新闻通讯 (Newsletter)](https://qdrant.tech/subscribe/)
- 正在寻找全托管云服务？查看[产品定价](https://qdrant.tech/pricing/)；如需个性化定制支持，欢迎联系 [info@qdrant.tech](mailto:info@qdrant.tech)

## 开源协议

Qdrant 基于 Apache License 2.0 许可证开源。请查看 [License 协议文件](https://github.com/qdrant/qdrant/blob/master/LICENSE)。

---

> 💡 **文档维护说明**：本中文文档由社区志愿者（[@JasonYeYuhe](https://github.com/JasonYeYuhe)）翻译维护，最后同步更新于 2026年9月6日。如发现内容与官方英文原版存在差异或新特性滞后，欢迎提交 PR 共同完善！
