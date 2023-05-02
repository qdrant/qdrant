# Roadmap 2022

This document describes what features and milestones were planned and achieved in 2022.

The main goals for the 2022 year were:

* **Make API and Storage stable** - ensure backward compatibility for at least one major version back.
    * Starting from the release, breaking changes in API should only be done with a proper deprecation notice
    * Storage should be compatible between any two consequent major versions
* **Achieve horizontal scalability** - distributed deployment able to serve billions of points
* **Easy integration** - make the user experience as smooth as possible
* **Resource efficiency** - push Qdrant performance on the single machine to the limit


## Milestones

* :earth_americas: Distributed Deployment
    * [x] Distributed querying
    * [x] Integration of [raft](https://raft.github.io/) for distributed consistency
    * [x] Sharding - group segments into shards
    * [x] Cluster scaling
    * [x] Replications - automatic segment replication between nodes in cluster

---

* :electric_plug: Integration & Interfaces
    * [x] gPRC version of each REST API endpoint
    * [x] Split REST Endpoints for better documentation and client generation

---

* :truck: Payload Processing
    * [x] Support storing any JSON as a Payload
    * [ ] ~~Support more payload types, e.g.~~
        * ~~Data-time~~
    * [x] Support for `Null` values
    * [x] Enable more types of filtering queries, e.g.
        * [x] Filter by Score
        * [x] Filter by number of stored elements
        * [x] `isNull` or `isEmpty` query conditions


* Additionally
    * [x] Full-text filtering support
    * [x] Multiple vectors per record support

---

* :racing_car: Performance improvements
    * [x] Indexing of geo-payload
    * [x] On the fly payload index
    * [x] Multiprocessing segment optimization
    * [x] Fine-tuned HNSW index configuration
  
