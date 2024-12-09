<p align="center">
  <img height="100" src="https://github.com/qdrant/qdrant/raw/master/docs/logo.svg" alt="Qdrant">
</p>

<p align="center">
    <b>Vector Search Engine for the next generation of AI applications</b>
</p>

<p align=center>
    <a href="https://github.com/qdrant/qdrant/actions/workflows/rust.yml"><img src="https://img.shields.io/github/actions/workflow/status/qdrant/qdrant/rust.yml?style=flat-square" alt="Tests status"></a>
    <a href="https://api.qdrant.tech/"><img src="https://img.shields.io/badge/Docs-OpenAPI%203.0-success?style=flat-square" alt="OpenAPI Docs"></a>
    <a href="https://github.com/qdrant/qdrant/blob/master/LICENSE"><img src="https://img.shields.io/github/license/qdrant/qdrant?style=flat-square" alt="Apache 2.0 License"></a>
    <a href="https://qdrant.to/discord"><img src="https://img.shields.io/discord/907569970500743200?logo=Discord&style=flat-square&color=7289da" alt="Discord"></a>
    <a href="https://qdrant.to/roadmap"><img src="https://img.shields.io/badge/Roadmap-2024-bc1439.svg?style=flat-square" alt="Roadmap 2024"></a>
    <a href="https://cloud.qdrant.io/"><img src="https://img.shields.io/badge/Qdrant-Cloud-24386C.svg?logo=cloud&style=flat-square" alt="Qdrant Cloud"></a>
</p>

**Qdrant** (read: _quadrant_) is a vector similarity search engine and vector database.
It provides a production-ready service with a convenient API to store, search, and manage points—vectors with an additional payload
Qdrant is tailored to extended filtering support. It makes it useful for all sorts of neural-network or semantic-based matching, faceted search, and other applications.

Qdrant is written in Rust 🦀, which makes it fast and reliable even under high load. See [benchmarks](https://qdrant.tech/benchmarks/).

With Qdrant, embeddings or neural network encoders can be turned into full-fledged applications for matching, searching, recommending, and much more!

Qdrant is also available as a fully managed **[Qdrant Cloud](https://cloud.qdrant.io/)** ⛅ including a **free tier**.

<p align="center">
<strong><a href="docs/QUICK_START.md">Quick Start</a> • <a href="#clients">Client Libraries</a> • <a href="#demo-projects">Demo Projects</a> • <a href="#integrations">Integrations</a> • <a href="#contacts">Contact</a>

</strong>
</p>

## Getting Started

### Python

```
pip install qdrant-client
```

The python client offers a convenient way to start with Qdrant locally:

```python
from qdrant_client import QdrantClient
qdrant = QdrantClient(":memory:") # Create in-memory Qdrant instance, for testing, CI/CD
# OR
client = QdrantClient(path="path/to/db")  # Persists changes to disk, fast prototyping
```

### Client-Server

To experience the full power of Qdrant locally, run the container with this command:

```bash
docker run -p 6333:6333 qdrant/qdrant
```

Now you can connect to this with any client, including Python:

```python
qdrant = QdrantClient("http://localhost:6333") # Connect to existing Qdrant instance
```

Before deploying Qdrant to production, be sure to read our [installation](https://qdrant.tech/documentation/guides/installation/) and [security](https://qdrant.tech/documentation/guides/security/) guides.

### Clients

Qdrant offers the following client libraries to help you integrate it into your application stack with ease:

- Official:
  - [Go client](https://github.com/qdrant/go-client)
  - [Rust client](https://github.com/qdrant/rust-client)
  - [JavaScript/TypeScript client](https://github.com/qdrant/qdrant-js)
  - [Python client](https://github.com/qdrant/qdrant-client)
  - [.NET/C# client](https://github.com/qdrant/qdrant-dotnet)
  - [Java client](https://github.com/qdrant/java-client)
- Community:
  - [Elixir](https://hexdocs.pm/qdrant/readme.html)
  - [PHP](https://github.com/hkulekci/qdrant-php)
  - [Ruby](https://github.com/andreibondarev/qdrant-ruby)
  - [Java](https://github.com/metaloom/qdrant-java-client)

### Where do I go from here?

- [Quick Start Guide](docs/QUICK_START.md)
- End to End [Colab Notebook](https://colab.research.google.com/drive/1Bz8RSVHwnNDaNtDwotfPj0w7AYzsdXZ-?usp=sharing) demo with SentenceBERT and Qdrant
- Detailed [Documentation](https://qdrant.tech/documentation/) are great starting points
- [Step-by-Step Tutorial](https://qdrant.to/qdrant-tutorial) to create your first neural network project with Qdrant

## Demo Projects  <a href="https://replit.com/@qdrant"><img align="right" src="https://replit.com/badge/github/qdrant/qdrant" alt="Run on Repl.it"></a>

### Discover Semantic Text Search 🔍

Unlock the power of semantic embeddings with Qdrant, transcending keyword-based search to find meaningful connections in short texts. Deploy a neural search in minutes using a pre-trained neural network, and experience the future of text search. [Try it online!](https://qdrant.to/semantic-search-demo)

### Explore Similar Image Search - Food Discovery 🍕

There's more to discovery than text search, especially when it comes to food. People often choose meals based on appearance rather than descriptions and ingredients. Let Qdrant help your users find their next delicious meal using visual search, even if they don't know the dish's name. [Check it out!](https://qdrant.to/food-discovery)

### Master Extreme Classification - E-commerce Product Categorization 📺

Enter the cutting-edge realm of extreme classification, an emerging machine learning field tackling multi-class and multi-label problems with millions of labels. Harness the potential of similarity learning models, and see how a pre-trained transformer model and Qdrant can revolutionize e-commerce product categorization. [Play with it online!](https://qdrant.to/extreme-classification-demo)

<details>
<summary> More solutions </summary>

<table>
    <tr>
        <td width="30%">
            <img src="https://qdrant.tech/content/images/text_search.png">
        </td>
        <td width="30%">
            <img src="https://qdrant.tech/content/images/image_search.png">
        </td>
        <td width="30%">
            <img src="https://qdrant.tech/content/images/recommendations.png">
        </td>
    </tr>
    <tr>
        <td>
            Semantic Text Search
        </td>
        <td>
            Similar Image Search
        </td>
        <td>
            Recommendations
        </td>
    </tr>
</table>

<table>
    <tr>
        <td>
            <img width="300px" src="https://qdrant.tech/content/images/chat_bots.png">
        </td>
        <td>
            <img width="300px" src="https://qdrant.tech/content/images/matching_engines.png">
        </td>
        <td>
            <img width="300px" src="https://qdrant.tech/content/images/anomalies_detection.png">
        </td>
    </tr>
    <tr>
        <td>
            Chat Bots
        </td>
        <td>
            Matching Engines
        </td>
        <td>
            Anomaly Detection
        </td>
    </tr>
</table>

</details>

## API

### REST

Online OpenAPI 3.0 documentation is available [here](https://api.qdrant.tech/).
OpenAPI makes it easy to generate a client for virtually any framework or programming language.

You can also download raw OpenAPI [definitions](https://github.com/qdrant/qdrant/blob/master/docs/redoc/master/openapi.json).

### gRPC

For faster production-tier searches, Qdrant also provides a gRPC interface. You can find gRPC documentation [here](https://qdrant.tech/documentation/interfaces/#grpc-interface).

## Features

### Filtering and Payload

Qdrant can attach any JSON payloads to vectors, allowing for both the storage and filtering of data based on the values in these payloads.
Payload supports a wide range of data types and query conditions, including keyword matching, full-text filtering, numerical ranges, geo-locations, and more.

Filtering conditions can be combined in various ways, including `should`, `must`, and `must_not` clauses,
ensuring that you can implement any desired business logic on top of similarity matching.


### Hybrid Search with Sparse Vectors

To address the limitations of vector embeddings when searching for specific keywords, Qdrant introduces support for sparse vectors in addition to the regular dense ones.

Sparse vectors can be viewed as an generalization of BM25 or TF-IDF ranking. They enable you to harness the capabilities of transformer-based neural networks to weigh individual tokens effectively.


### Vector Quantization and On-Disk Storage

Qdrant provides multiple options to make vector search cheaper and more resource-efficient.
Built-in vector quantization reduces RAM usage by up to 97% and dynamically manages the trade-off between search speed and precision.


### Distributed Deployment

Qdrant offers comprehensive horizontal scaling support through two key mechanisms:
1. Size expansion via sharding and throughput enhancement via replication
2. Zero-downtime rolling updates and seamless dynamic scaling of the collections


### Highlighted Features

* **Query Planning and Payload Indexes** - leverages stored payload information to optimize query execution strategy.
* **SIMD Hardware Acceleration** - utilizes modern CPU x86-x64 and Neon architectures to deliver better performance.
* **Async I/O** - uses `io_uring` to maximize disk throughput utilization even on a network-attached storage.
* **Write-Ahead Logging** - ensures data persistence with update confirmation, even during power outages.


# Integrations

Examples and/or documentation of Qdrant integrations:

- [Cohere](https://docs.cohere.com/docs/qdrant-and-cohere) ([blogpost on building a QA app with Cohere and Qdrant](https://qdrant.tech/articles/qa-with-cohere-and-qdrant/)) - Use Cohere embeddings with Qdrant
- [DocArray](https://docs.docarray.org/user_guide/storing/index_qdrant/) - Use Qdrant as a document store in DocArray
- [Haystack](https://haystack.deepset.ai/integrations/qdrant-document-store) - Use Qdrant as a document store with Haystack ([blogpost](https://haystack.deepset.ai/blog/qdrant-integration)).
- [LangChain](https://python.langchain.com/docs/integrations/providers/qdrant/) ([blogpost](https://qdrant.tech/articles/langchain-integration/)) - Use Qdrant as a memory backend for LangChain.
- [LlamaIndex](https://gpt-index.readthedocs.io/en/latest/examples/vector_stores/QdrantIndexDemo.html) - Use Qdrant as a Vector Store with LlamaIndex.
- [OpenAI - ChatGPT retrieval plugin](https://github.com/openai/chatgpt-retrieval-plugin/blob/main/docs/providers/qdrant/setup.md) - Use Qdrant as a memory backend for ChatGPT
- [Microsoft Semantic Kernel](https://devblogs.microsoft.com/semantic-kernel/the-power-of-persistent-memory-with-semantic-kernel-and-qdrant-vector-database/) - Use Qdrant as persistent memory with Semantic Kernel

## Contacts

- Have questions? Join our [Discord channel](https://qdrant.to/discord) or mention [@qdrant_engine on Twitter](https://qdrant.to/twitter)
- Want to stay in touch with latest releases? Subscribe to our [Newsletters](https://qdrant.to/newsletter)
- Looking for a managed cloud? Check [pricing](https://qdrant.tech/pricing/), need something personalised? We're at [info@qdrant.tech](mailto:info@qdrant.tech)

## License

Qdrant is licensed under the Apache License, Version 2.0. View a copy of the [License file](https://github.com/qdrant/qdrant/blob/master/LICENSE).
