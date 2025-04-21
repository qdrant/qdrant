# Qdrant 2025 Roadmap

Hi!
This document is our plan for Qdrant development in 2025.
Previous year roadmap is available here:

* [Roadmap 2024](roadmap-2024.md)
* [Roadmap 2023](roadmap-2023.md)
* [Roadmap 2022](roadmap-2022.md)

Goals of the release:

* **Maintain easy upgrades** - we plan to keep backward compatibility for at least one minor version back (this stays the same in 2025)
  * We plan to maintain forward client compatibility for multiple minor versions. Version incompatibility will be announced in the release notes. Also, we will track version compatibility between server and client automatically, and notify you if something is out of sync.
  * That means that you can upgrade Qdrant without any downtime and without any changes in your client code within one or more minor versions.
  * Storage should be compatible between any two consequent versions, so you can upgrade Qdrant with automatic data migration between consecutive versions.
  * We plan to eventually migrate away from RocksDB, so at some point we might introduce data migration into the upgrade process.
* **Continue to improve scalability** - Qdrant should be a go-to solution if you are running a billion-scale vector search. We already progressed a lot in this area, but there are still a few places where we can improve.
* **Focus on vector similarity** - we believe that vector similarity is not only about search. Vector similarity reveals the structure of your data, even if you don't have specific search queries. We plan to introduce more functions, demos and tutorials targeting this use-case.
* **End-to-end processing** - We plan to introduce tighter integration with embedding providers, so close gap between local development and production deployment.
* **Serverless** - In 2025 we plan to make Qdrant serverless-ready. Unlike other solutions, we don't plan to make a separate serverless product, but to make Qdrant itself serverless-ready. This means that you can run Qdrant in a serverless environment and pay only for the resources you use.


## How to contribute

If you are a Qdrant user - Data Scientist, ML Engineer, or MLOps, the best contribution would be the feedback on your experience with Qdrant.
Let us know whenever you have a problem, face an unexpected behavior, or see a lack of documentation.
You can do it in any convenient way - create an [issue](https://github.com/qdrant/qdrant/issues), start a [discussion](https://github.com/qdrant/qdrant/discussions), or drop up a [message](https://discord.gg/tdtYvXjC4h).
If you use Qdrant or Metric Learning in your projects, we'd love to hear your story! Feel free to share articles and demos in our community.

For those familiar with Rust - check out our [contribution guide](../CONTRIBUTING.md).
If you have problems with code or architecture understanding - reach us at any time.
Feeling confident and want to contribute more? - Come to [work with us](https://qdrant.join.com/)!

## Core Milestones

* 🔭 Search and Discovery
  * [ ] Diversity Sampling
  * [ ] Server-side embedding inference
  * [x] Built-in score boosting
  * [ ] Advanced text filtering
    * [ ] Phrase queries
    * [ ] Logical operators
    * [ ] Stemmers

---

* 🏗️ Scalability
  * [ ] Read/write segregation advanced support
    * [ ] Read-only nodes
    * [ ] Incremental snapshots
    * [ ] Multi-region replication
  * [ ] S3 as a vector&payload storage

---

* ⚙️ Performance
  * [ ] Incremental HNSW indexing
  * [ ] Built-in TTL support

