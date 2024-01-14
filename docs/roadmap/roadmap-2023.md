# Qdrant 2023 Roadmap

Hi!
This document is our plan for Qdrant development in 2023.
Previous year roadmap is available here:

* [Roadmap 2022](roadmap-2022.md)

Goals of the release:

* **Maintain easy upgrades** - we plan to keep backward compatibility for at least one minor version back.
  * That means that you can upgrade Qdrant without any downtime and without any changes in your client code within one minor version.
  * Storage should be compatible between any two consequent versions, so you can upgrade Qdrant with automatic data migration between consecutive versions.
* **Make billion-scale serving cheap** - qdrant already can serve billions of vectors, but we want to make it even more affordable.
* **Easy scaling** - our plan is to make it easy to dynamically scale Qdrant, so you could go from 1 to 1B vectors seamlessly.
* **Various similarity search scenarios** - we want to support more similarity search scenarios, e.g. sparse search, grouping requests, diverse search, etc.

## How to contribute

If you are a Qdrant user - Data Scientist, ML Engineer, or MLOps, the best contribution would be the feedback on your experience with Qdrant.
Let us know whenever you have a problem, face an unexpected behavior, or see a lack of documentation.
You can do it in any convenient way - create an [issue](https://github.com/qdrant/qdrant/issues), start a [discussion](https://github.com/qdrant/qdrant/discussions), or drop up a [message](https://discord.gg/tdtYvXjC4h).
If you use Qdrant or Metric Learning in your projects, we'd love to hear your story! Feel free to share articles and demos in our community.

For those familiar with Rust - check out our [contribution guide](https://github.com/qdrant/qdrant/blob/master/CONTRIBUTING.md).
If you have problems with code or architecture understanding - reach us at any time.
Feeling confident and want to contribute more? - Come to [work with us](https://qdrant.join.com/)!

## Milestones

* :atom_symbol: Quantization support
  * [x] Scalar quantization f32 -> u8 (4x compression)
  * [x] Product quantization (4x, 8x, 16x, 32x, and 64x compression)
  * [x] Binary quantization (32x compression, 40x speedup)
  * [x] Support for binary vectors

---

* :arrow_double_up: Scalability
  * [ ] Automatic replication factor adjustment
  * [ ] Automatic shard distribution on cluster scaling
  * [x] Repartitioning support

---

* :eyes: Search scenarios
  * [ ] Diversity search - search for vectors that are different from each other
  * [x] Discovery search - constrain the space in which the search is performed
  * [x] Sparse vectors search - search for vectors with a small number of non-zero values
  * [x] Grouping requests - search within payload-defined groups
  * [x] Different scenarios for recommendation API

---

* Additionally
  * [ ] Extend full-text filtering support
    * [ ] Support for phrase queries
    * [ ] Support for logical operators
  * [x] Simplify update of collection parameters