ConditionType: TypeAlias = (
    FieldCondition
    | Filter
    | HasIdCondition
    | HasVectorCondition
    | IsEmptyCondition
    | IsNullCondition
    | NestedCondition
    | SliceCondition
)
GeoLineString: TypeAlias = Sequence[GeoPoint]
IndexType: TypeAlias = HnswIndexConfig | PlainIndexConfig
JsonPath: TypeAlias = str
MatchType: TypeAlias = (
    MatchAny
    | MatchExcept
    | MatchPhrase
    | MatchPrefix
    | MatchText
    | MatchTextAny
    | MatchValue
)
NamedVector: TypeAlias = Sequence[Sequence[float]] | Sequence[float] | SparseVector
Payload: TypeAlias = dict[str, Any]
PayloadFieldSchema: TypeAlias = PayloadSchemaParams | PayloadSchemaType
PayloadSchemaParams: TypeAlias = (
    BoolIndexParams
    | DatetimeIndexParams
    | FloatIndexParams
    | GeoIndexParams
    | IntegerIndexParams
    | KeywordIndexParams
    | TextIndexParams
    | UuidIndexParams
)
PointId: TypeAlias = int | str | uuid.UUID
QuantizationConfigType: TypeAlias = (
    BinaryQuantizationConfig
    | ProductQuantizationConfig
    | ScalarQuantizationConfig
    | TurboQuantQuantizationConfig
)
ScoringQueryType: TypeAlias = Formula | Fusion | Mmr | OrderBy | Query | Sample
StartFromType: TypeAlias = float | int | str
StemmingAlgorithm: TypeAlias = DisabledStemmer | SnowballParams
Stopwords: TypeAlias = Language | StopwordsSet
Vector: TypeAlias = Sequence[Sequence[float]] | Sequence[float] | dict[str, NamedVector]
WithPayloadType: TypeAlias = PayloadSelector | Sequence[JsonPath] | bool
WithVectorType: TypeAlias = Sequence[str] | bool

@final
class AcornSearchParams:
    """Parameters for Acorn filtered search."""

    def __new__(enable: bool = False, max_selectivity: None | float = None):
        """Create AcornSearchParams.

        Args:
            enable: Whether to enable Acorn.
            max_selectivity: Maximum filter selectivity for Acorn."""

    def __repr__() -> str: ...
    @property
    def enable() -> bool:
        """Enable flag."""

    @property
    def max_selectivity() -> None | float:
        """Maximum selectivity."""

@final
class BinaryQuantizationConfig:
    """Configuration for binary quantization."""

    def __new__(
        always_ram: None | bool = None,
        encoding: BinaryQuantizationEncoding | None = None,
        query_encoding: BinaryQuantizationQueryEncoding | None = None,
    ):
        """Create a BinaryQuantizationConfig.

        Args:
            always_ram: Whether to keep in RAM.
            encoding: Binary encoding type.
            query_encoding: Query encoding type."""

    def __repr__() -> str: ...
    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def encoding() -> BinaryQuantizationEncoding | None:
        """Encoding."""

    @property
    def query_encoding() -> BinaryQuantizationQueryEncoding | None:
        """Query encoding."""

@final
class BinaryQuantizationEncoding:
    """Binary quantization encoding types."""

    OneAndHalfBits: Final[BinaryQuantizationEncoding]
    OneBit: Final[BinaryQuantizationEncoding]
    TwoBits: Final[BinaryQuantizationEncoding]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class BinaryQuantizationQueryEncoding:
    """Binary quantization query encoding types."""

    Binary: Final[BinaryQuantizationQueryEncoding]
    Default: Final[BinaryQuantizationQueryEncoding]
    Scalar4Bits: Final[BinaryQuantizationQueryEncoding]
    Scalar8Bits: Final[BinaryQuantizationQueryEncoding]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class Bm25:
    """BM25 sparse-vector embedding model."""

    def __new__(config: Bm25Config | None = None):
        """Create a Bm25 model with the given configuration (defaults if `None`).

        Raises `ValueError` for invalid configuration: unsupported `language`,
        non-positive `avg_len`, `b` outside `[0.0, 1.0]`, or negative `k`."""

    def embed_document(text: str) -> SparseVector:
        """Embed `text` as an indexed document: term-frequency weights with
        `(k, b, avg_len)` from the model config."""

    def embed_query(text: str) -> SparseVector:
        """Embed `text` as a search query: each unique token gets weight 1.0."""

@final
class Bm25Config:
    """Configuration for an edge-side BM25 model."""

    def __new__(
        k: None | float = None,
        b: None | float = None,
        avg_len: None | float = None,
        tokenizer: None | TokenizerType = None,
        language: None | str = None,
        lowercase: None | bool = None,
        ascii_folding: None | bool = None,
        stopwords: None | Stopwords = None,
        stemmer: None | StemmingAlgorithm = None,
        min_token_len: None | int = None,
        max_token_len: None | int = None,
    ):
        """Create a Bm25Config.

        Args:
            k: Term-frequency saturation. Higher = TF has more impact. Default 1.2.
            b: Length normalization. 0=none, 1=full. Default 0.75.
            avg_len: Expected average document length in tokens. Default 256.
            tokenizer: Tokenizer type to use.
            language: Language for default stopwords/stemmer (e.g., "english").
            lowercase: Lowercase before tokenization. Default True.
            ascii_folding: Fold accents to ASCII. Default False.
            stopwords: Custom stopwords (language or set). Defaults to language.
            stemmer: Stemming algorithm. Defaults to language-appropriate stemmer.
            min_token_len: Drop tokens shorter than this.
            max_token_len: Drop tokens longer than this."""

    @property
    def ascii_folding() -> None | bool: ...
    @property
    def avg_len() -> float: ...
    @property
    def b() -> float: ...
    @property
    def k() -> float: ...
    @property
    def language() -> None | str: ...
    @property
    def lowercase() -> None | bool: ...
    @property
    def max_token_len() -> None | int: ...
    @property
    def min_token_len() -> None | int: ...
    @property
    def stemmer() -> None | StemmingAlgorithm: ...
    @property
    def stopwords() -> None | Stopwords: ...
    @property
    def tokenizer() -> TokenizerType: ...

@final
class BoolIndexParams:
    """Index parameters for boolean fields."""

    def __new__(on_disk: None | bool = None, enable_hnsw: None | bool = None):
        """Create BoolIndexParams.

        Args:
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

@final
class CompressionRatio:
    """Product quantization compression ratios."""

    X16: Final[CompressionRatio]
    X32: Final[CompressionRatio]
    X4: Final[CompressionRatio]
    X64: Final[CompressionRatio]
    X8: Final[CompressionRatio]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class ContextPair:
    """A positive/negative pair for context-based queries."""

    def __new__(positive: NamedVector, negative: NamedVector):
        """Create a ContextPair.

        Args:
            positive: Positive example.
            negative: Negative example."""

    def __repr__() -> str: ...
    @property
    def negative() -> NamedVector:
        """Negative example."""

    @property
    def positive() -> NamedVector:
        """Positive example."""

@final
class ContextQuery:
    """Query based on context pairs only."""

    def __new__(pairs: Sequence[ContextPair]):
        """Create a ContextQuery.

        Args:
            pairs: Context pairs."""

    def __repr__() -> str: ...
    @property
    def pairs() -> list[ContextPair]:
        """Context pairs."""

@final
class CountRequest:
    """Request for count operation."""

    def __new__(exact: bool = True, filter: Filter | None = None):
        """Create a CountRequest.

        Args:
            exact: Whether to count exactly or estimate.
            filter: Filter conditions."""

    @property
    def exact() -> bool:
        """Exact count flag."""

    @property
    def filter() -> Filter | None:
        """Filter."""

@final
class DatetimeIndexParams:
    """Index parameters for datetime fields."""

    def __new__(
        is_principal: None | bool = None,
        on_disk: None | bool = None,
        enable_hnsw: None | bool = None,
    ):
        """Create DatetimeIndexParams.

        Args:
            is_principal: Whether this field is a principal identifier.
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def is_principal() -> None | bool:
        """Whether this field is a principal identifier."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

@final
class DecayKind:
    """Decay function kinds for scoring formulas."""

    Exp: Final[DecayKind]
    """
    Exponential decay function
    """
    Gauss: Final[DecayKind]
    """
    Gaussian decay function
    """
    Lin: Final[DecayKind]
    """
    Linear decay function
    """

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class Direction:
    """Sort direction."""

    Asc: Final[Direction]
    Desc: Final[Direction]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class DisabledStemmer:
    """Explicitly disable stemming, overriding the language default."""

    def __new__():
        """Create a DisabledStemmer."""

@final
class DiscoverQuery:
    """Query for discovery using a target and context pairs."""

    def __new__(target: NamedVector, pairs: Sequence[ContextPair]):
        """Create a DiscoverQuery.

        Args:
            target: Target vector.
            pairs: Context pairs."""

    def __repr__() -> str: ...
    @property
    def pairs() -> list[ContextPair]:
        """Context pairs."""

    @property
    def target() -> NamedVector:
        """Target vector."""

@final
class Distance:
    """Distance metrics for vector comparison."""

    Cosine: Final[Distance]
    Dot: Final[Distance]
    Euclid: Final[Distance]
    Manhattan: Final[Distance]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class EdgeConfig:
    """Configuration for creating a new Qdrant Edge shard."""

    def __new__(
        vectors: EdgeVectorParams | None | dict[str, EdgeVectorParams] = None,
        sparse_vectors: None | dict[str, EdgeSparseVectorParams] = None,
        on_disk_payload: None | bool = None,
        hnsw_config: HnswIndexConfig | None = None,
        quantization_config: None | QuantizationConfigType = None,
        optimizers: EdgeOptimizersConfig | None = None,
        max_search_threads: None | int = None,
        search_pool_core: None | int = None,
    ):
        """Create an EdgeConfig.

        Parameters left as None are "not specified": when loading an existing shard each
        one resolves through provided -> persisted -> derived from segments -> default,
        so an unspecified parameter keeps the shard as it is. vectors and sparse_vectors
        define the stored data: if provided they are validated for compatibility against
        the existing segments, if omitted they are inherited from the shard.

        Args:
            vectors: Dense vector configuration. Can be a single EdgeVectorParams for
                     the default vector (name "") or a dict of name -> EdgeVectorParams.
                     Optional if sparse_vectors is provided (sparse-only config).
            sparse_vectors: Optional sparse vector configurations.
            on_disk_payload: If True, store payload on disk (mmap); otherwise in RAM.
                             None keeps the shard's current value (defaults to on-disk).
            hnsw_config: Optional global HNSW config (used when building HNSW index).
            quantization_config: Optional global quantization config.
            optimizers: Optional optimizer settings.
            max_search_threads: Number of threads in the shard's search thread pool, which
                                runs per-segment reads in parallel and loads segments in
                                parallel. None (the default) derives the count from the number
                                of CPUs, matching the core search runtime.
            search_pool_core: Pin every search pool thread to this CPU core (best-effort),
                              bounding search compute to one core. None = OS scheduling.
        """

    def __repr__() -> str: ...
    @property
    def hnsw_config() -> HnswIndexConfig | None:
        """Global HNSW config, or None if not specified."""

    @property
    def max_search_threads() -> None | int:
        """Number of threads in the search thread pool, or None for the CPU-derived default."""

    @property
    def on_disk_payload() -> None | bool:
        """Whether payload is stored on disk, or None if not specified."""

    @property
    def optimizers() -> EdgeOptimizersConfig | None:
        """Optimizer settings, or None if not specified."""

    @property
    def quantization_config() -> None | QuantizationConfigType:
        """Global quantization config."""

    @property
    def search_pool_core() -> None | int:
        """CPU core the search pool is pinned to, or None for OS scheduling."""

    @property
    def sparse_vectors() -> dict[str, EdgeSparseVectorParams]:
        """Sparse vector configurations."""

    @property
    def vectors() -> dict[str, EdgeVectorParams]:
        """Dense vector configurations."""

@final
class EdgeOptimizersConfig:
    """Optimizer-related configuration for EdgeConfig."""

    def __new__(
        deleted_threshold: None | float = None,
        vacuum_min_vector_number: None | int = None,
        default_segment_number: None | int = None,
        max_segment_size: None | int = None,
        indexing_threshold: None | int = None,
        prevent_unoptimized: None | bool = None,
    ):
        """Create EdgeOptimizersConfig.

        Args:
            deleted_threshold: Min fraction of deleted vectors to run vacuum (default 0.2).
            vacuum_min_vector_number: Min vectors in segment to run vacuum (default 1000).
            default_segment_number: Target number of segments (0 = auto).
            max_segment_size: Max segment size in KB.
            indexing_threshold: Indexing threshold in KB.
            prevent_unoptimized: If enabled, points written to segments larger than the indexing threshold
                become deferred (excluded from read/search until those segments are optimized).
                Updates with `wait=true` will only return after the deferred points become visible.
        """

    def __repr__() -> str: ...
    @property
    def default_segment_number() -> None | int:
        """Default segment number."""

    @property
    def deleted_threshold() -> None | float:
        """Deleted threshold."""

    @property
    def indexing_threshold() -> None | int:
        """Indexing threshold in KB."""

    @property
    def max_segment_size() -> None | int:
        """Max segment size in KB."""

    @property
    def prevent_unoptimized() -> None | bool:
        """Prevent unoptimized flag."""

    @property
    def vacuum_min_vector_number() -> None | int:
        """Vacuum min vector number."""

@final
class EdgeShard:
    """The main class representing a Qdrant Edge shard.

    A shard is a self-contained unit of storage that can be loaded, queried,
    and updated independently. Use load() to open existing data, or create()
    to create a new shard."""

    def close() -> None:
        """Close the shard and release all resources."""

    def count(count: CountRequest) -> int:
        """Count points in the shard.

        Args:
            count: The count request.

        Returns:
            Number of points matching the filter."""

    @staticmethod
    def create(path: PathLike[str] | str, config: EdgeConfig) -> EdgeShard:
        """Create a new edge shard at path with the given configuration.
        Fails if the path already contains segment data.

        Args:
            path: Path to the shard directory (must not contain existing segments).
            config: Configuration for the new shard.

        Returns:
            New EdgeShard instance."""

    def facet(facet: FacetRequest) -> FacetResponse:
        """Get facets for a payload field.

        Args:
            facet: The facet request.

        Returns:
            Facet response with hits and counts."""

    def flush() -> None:
        """Flush all pending changes to disk."""

    def info() -> ShardInfo:
        """Get information about the shard.

        Returns:
            Shard information."""

    @staticmethod
    def load(path: PathLike[str] | str, config: EdgeConfig | None = None) -> EdgeShard:
        """Load an edge shard from existing files at path.

        Args:
            path: Path to the shard directory.
            config: Optional; if provided, compatibility is checked and config
                    is overwritten on disk.

        Returns:
            Loaded EdgeShard instance."""

    def optimize() -> bool:
        """Run segment optimizers in-process, blocking until no more optimizations are planned.

        Returns:
            True if any segments were optimized, False if already optimal."""

    def query(query: QueryRequest) -> list[ScoredPoint]:
        """Execute a query against the shard.

        Args:
            query: The query request.

        Returns:
            List of scored points matching the query."""

    def query_batch(request: QueryBatchRequest) -> list[list[ScoredPoint]]:
        """Execute several queries as one planned batch.

        Cheaper than calling `query` once per request: the batch is planned as a
        whole, so its searches share one pass over the segments and queries that
        differ only in their vector are scored together.

        Args:
            request: The batch of query requests to run together.

        Returns:
            One list of scored points per request, in the same order."""

    def retrieve(
        point_ids: Sequence[PointId],
        with_payload: None | WithPayloadType = None,
        with_vector: None | WithVectorType = None,
    ) -> list[Record]:
        """Retrieve specific points by their IDs.

        Args:
            point_ids: List of point IDs to retrieve.
            with_payload: Whether to include payload in results.
            with_vector: Whether to include vectors in results.

        Returns:
            List of records."""

    def scroll(scroll: ScrollRequest) -> tuple[list[Record], None | PointId]:
        """Scroll through points in the shard.

        Args:
            scroll: The scroll request.

        Returns:
            Tuple of (points, next_offset)."""

    def search(search: SearchRequest) -> list[ScoredPoint]:
        """Execute a search against the shard.

        Args:
            search: The search request.

        Returns:
            List of scored points matching the search."""

    def snapshot_manifest() -> Any:
        """Get the snapshot manifest.

        Returns:
            Snapshot manifest as a JSON-like value."""

    @staticmethod
    def unpack_snapshot(
        snapshot_path: PathLike[str] | str, target_path: PathLike[str] | str
    ) -> None:
        """Unpack a snapshot to a target directory.

        Args:
            snapshot_path: Path to the snapshot file.
            target_path: Path to extract the snapshot to."""

    def update(operation: UpdateOperation) -> None:
        """Apply an update operation to the shard.

        Args:
            operation: The update operation to apply."""

    def update_from_snapshot(
        snapshot_path: PathLike[str] | str, tmp_dir: None | PathLike[str] | str = None
    ) -> None:
        """Update the shard from a snapshot.

        Args:
            snapshot_path: Path to the snapshot file.
            tmp_dir: Optional temporary directory for extraction."""

@final
class EdgeSparseVectorParams:
    """Sparse vector parameters for EdgeConfig."""

    def __new__(
        full_scan_threshold: None | int = None,
        on_disk: None | bool = None,
        modifier: Modifier | None = None,
        datatype: None | VectorStorageDatatype = None,
    ):
        """Create EdgeSparseVectorParams.

        Args:
            full_scan_threshold: Threshold for full scan vs index search.
            on_disk: If True, sparse index on disk; otherwise in RAM.
            modifier: Optional modifier (e.g., IDF).
            datatype: Storage datatype."""

    def __repr__() -> str: ...
    @property
    def datatype() -> None | VectorStorageDatatype:
        """Storage datatype."""

    @property
    def full_scan_threshold() -> None | int:
        """Full scan threshold."""

    @property
    def modifier() -> Modifier | None:
        """Modifier."""

    @property
    def on_disk() -> None | bool:
        """Whether sparse index is on disk."""

@final
class EdgeVectorParams:
    """Dense vector parameters for EdgeConfig."""

    def __new__(
        size: int,
        distance: Distance,
        on_disk: None | bool = None,
        multivector_config: MultiVectorConfig | None = None,
        datatype: None | VectorStorageDatatype = None,
        quantization_config: None | QuantizationConfigType = None,
        hnsw_config: HnswIndexConfig | None = None,
    ):
        """Create EdgeVectorParams.

        Args:
            size: Dimension of vectors.
            distance: Distance metric.
            on_disk: If True, store vectors on disk (mmap); otherwise in RAM.
            multivector_config: Optional multi-vector configuration.
            datatype: Optional storage datatype.
            quantization_config: Optional per-vector quantization override.
            hnsw_config: Optional per-vector HNSW config override."""

    def __repr__() -> str: ...
    @property
    def datatype() -> None | VectorStorageDatatype:
        """Storage datatype."""

    @property
    def distance() -> Distance:
        """Distance metric."""

    @property
    def hnsw_config() -> HnswIndexConfig | None:
        """HNSW config override."""

    @property
    def multivector_config() -> MultiVectorConfig | None:
        """Multi-vector configuration."""

    @property
    def on_disk() -> None | bool:
        """Whether vector storage is on disk."""

    @property
    def quantization_config() -> None | QuantizationConfigType:
        """Quantization configuration."""

    @property
    def size() -> int:
        """Vector dimension."""

class Expression:
    """Expression types for formulas."""

    @final
    class Abs(Expression):
        """Create an absolute value expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Acosh(Expression):
        """Create an inverse hyperbolic cosine expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Condition(Expression):
        """Create a condition expression (returns 1 if true, 0 if false)."""

        __match_args__: Final = ("cond",)

        def __new__(cond: ConditionType): ...
        @property
        def cond() -> ConditionType: ...

    @final
    class Constant(Expression):
        """Create a constant expression."""

        __match_args__: Final = ("val",)

        def __new__(val: float): ...
        @property
        def val() -> float: ...

    @final
    class Datetime(Expression):
        """Create a datetime constant expression."""

        __match_args__: Final = ("date_time",)

        def __new__(date_time: str): ...
        @property
        def date_time() -> str: ...

    @final
    class DatetimeKey(Expression):
        """Create a datetime field expression."""

        __match_args__: Final = ("path",)

        def __new__(path: JsonPath): ...
        @property
        def path() -> JsonPath: ...

    @final
    class Decay(Expression):
        """Create a decay expression."""

        __match_args__: Final = ("kind", "x", "target", "midpoint", "scale")

        def __new__(
            kind: DecayKind,
            x: Expression,
            target: Expression | None,
            midpoint: None | float,
            scale: None | float,
        ): ...
        @property
        def kind() -> DecayKind: ...
        @property
        def midpoint() -> None | float: ...
        @property
        def scale() -> None | float: ...
        @property
        def target() -> Expression | None: ...
        @property
        def x() -> Expression: ...

    @final
    class Div(Expression):
        """Create a division expression."""

        __match_args__: Final = ("left", "right", "by_zero_default")

        def __new__(
            left: Expression, right: Expression, by_zero_default: None | float
        ): ...
        @property
        def by_zero_default() -> None | float: ...
        @property
        def left() -> Expression: ...
        @property
        def right() -> Expression: ...

    @final
    class Exp(Expression):
        """Create an exponential expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class GeoDistance(Expression):
        """Create a geo distance expression."""

        __match_args__: Final = ("origin", "to")

        def __new__(origin: GeoPoint, to: JsonPath): ...
        @property
        def origin() -> GeoPoint: ...
        @property
        def to() -> JsonPath: ...

    @final
    class Ln(Expression):
        """Create a natural log expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Log10(Expression):
        """Create a log10 expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Max(Expression):
        """Create a maximum expression. Requires at least one operand."""

        __match_args__: Final = ("exprs",)

        def __new__(exprs: Sequence[Expression]): ...
        @property
        def exprs() -> list[Expression]: ...

    @final
    class Min(Expression):
        """Create a minimum expression. Requires at least one operand."""

        __match_args__: Final = ("exprs",)

        def __new__(exprs: Sequence[Expression]): ...
        @property
        def exprs() -> list[Expression]: ...

    @final
    class Mult(Expression):
        """Create a multiplication expression."""

        __match_args__: Final = ("exprs",)

        def __new__(exprs: Sequence[Expression]): ...
        @property
        def exprs() -> list[Expression]: ...

    @final
    class Neg(Expression):
        """Create a negation expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Pow(Expression):
        """Create a power expression."""

        __match_args__: Final = ("base", "exponent")

        def __new__(base: Expression, exponent: Expression): ...
        @property
        def base() -> Expression: ...
        @property
        def exponent() -> Expression: ...

    @final
    class Sqrt(Expression):
        """Create a square root expression."""

        __match_args__: Final = ("expr",)

        def __new__(expr: Expression): ...
        @property
        def expr() -> Expression: ...

    @final
    class Sum(Expression):
        """Create a sum expression."""

        __match_args__: Final = ("exprs",)

        def __new__(exprs: Sequence[Expression]): ...
        @property
        def exprs() -> list[Expression]: ...

    @final
    class Variable(Expression):
        """Create a variable expression."""

        __match_args__: Final = ("var",)

        def __new__(var: str): ...
        @property
        def var() -> str: ...

@final
class FacetHit:
    """A facet hit with value and count."""

    def __repr__() -> str: ...
    @property
    def count() -> int:
        """Count of points with this value."""

    @property
    def value() -> bool | int | str:
        """Facet value."""

@final
class FacetRequest:
    """Request for facet operation."""

    def __new__(
        key: JsonPath,
        limit: int = 10,
        exact: bool = False,
        filter: Filter | None = None,
    ):
        """Create a FacetRequest.

        Args:
            key: Payload field key to facet on.
            limit: Maximum number of facet hits to return.
            exact: Whether to count exactly or estimate.
            filter: Filter conditions."""

    @property
    def exact() -> bool:
        """Exact count flag."""

    @property
    def filter() -> Filter | None:
        """Filter."""

    @property
    def key() -> JsonPath:
        """Facet key."""

    @property
    def limit() -> int:
        """Result limit."""

@final
class FacetResponse:
    """Response for facet operation."""

    def __iter__() -> Any:
        """Iterate over hits."""

    def __len__() -> int:
        """Number of hits."""

    def __repr__() -> str: ...
    @property
    def hits() -> list[FacetHit]:
        """Facet hits."""

@final
class FeedbackItem:
    """A feedback item with vector and score."""

    def __new__(vector: NamedVector, score: float):
        """Create a FeedbackItem.

        Args:
            vector: Feedback vector.
            score: Feedback score."""

    def __repr__() -> str: ...
    @property
    def score() -> float:
        """Feedback score."""

    @property
    def vector() -> NamedVector:
        """Feedback vector."""

@final
class FeedbackNaiveQuery:
    """Query using naive feedback approach."""

    def __new__(
        target: NamedVector,
        feedback: Sequence[FeedbackItem],
        strategy: NaiveFeedbackStrategy,
    ):
        """Create a FeedbackNaiveQuery.

        Args:
            target: Target vector.
            feedback: Feedback items with scores.
            strategy: Feedback coefficients."""

    def __repr__() -> str: ...
    @property
    def coefficients() -> NaiveFeedbackStrategy:
        """Coefficients."""

    @property
    def feedback() -> list[FeedbackItem]:
        """Feedback items."""

    @property
    def target() -> NamedVector:
        """Target vector."""

@final
class FieldCondition:
    """Condition on a payload field."""

    def __new__(
        key: JsonPath,
        match: MatchType | None = None,
        range: None | RangeDateTime | RangeFloat = None,
        geo_bounding_box: GeoBoundingBox | None = None,
        geo_radius: GeoRadius | None = None,
        geo_polygon: GeoPolygon | None = None,
        values_count: None | ValuesCount = None,
        is_empty: None | bool = None,
        is_null: None | bool = None,
    ):
        """Create a FieldCondition.

        Args:
            key: Payload field path.
            match: Match condition.
            range: Range condition.
            geo_bounding_box: Geo bounding box condition.
            geo_radius: Geo radius condition.
            geo_polygon: Geo polygon condition.
            values_count: Values count condition.
            is_empty: Check if empty.
            is_null: Check if null."""

    @property
    def geo_bounding_box() -> GeoBoundingBox | None:
        """Geo bounding box."""

    @property
    def geo_polygon() -> GeoPolygon | None:
        """Geo polygon."""

    @property
    def geo_radius() -> GeoRadius | None:
        """Geo radius."""

    @property
    def is_empty() -> None | bool:
        """Is empty flag."""

    @property
    def is_null() -> None | bool:
        """Is null flag."""

    @property
    def key() -> JsonPath:
        """Field key."""

    @property
    def match() -> MatchType | None:
        """Match condition."""

    @property
    def range() -> None | RangeDateTime | RangeFloat:
        """Range condition."""

    @property
    def values_count() -> None | ValuesCount:
        """Values count."""

@final
class Filter:
    """Filter conditions for queries."""

    def __new__(
        must: None | Sequence[ConditionType] = None,
        should: None | Sequence[ConditionType] = None,
        must_not: None | Sequence[ConditionType] = None,
        min_should: MinShould | None = None,
    ):
        """Create a Filter.

        Args:
            must: Conditions that must all match.
            should: Conditions where at least one should match.
            must_not: Conditions that must not match.
            min_should: Minimum number of should conditions to match."""

    def __repr__() -> str: ...
    @property
    def min_should() -> MinShould | None:
        """Minimum should configuration."""

    @property
    def must() -> None | list[ConditionType]:
        """Must conditions."""

    @property
    def must_not() -> None | list[ConditionType]:
        """Must not conditions."""

    @property
    def should() -> None | list[ConditionType]:
        """Should conditions."""

@final
class FloatIndexParams:
    """Index parameters for float fields."""

    def __new__(
        is_principal: None | bool = None,
        on_disk: None | bool = None,
        enable_hnsw: None | bool = None,
    ):
        """Create FloatIndexParams.

        Args:
            is_principal: Whether this field is a principal identifier.
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def is_principal() -> None | bool:
        """Whether this field is a principal identifier."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

@final
class Formula:
    """A scoring formula for custom ranking."""

    def __new__(formula: Expression, defaults: None | dict[str, Any] = None):
        """Create a Formula.

        Args:
            formula: Expression tree.
            defaults: Default variable values."""

    def __repr__() -> str: ...

class Fusion:
    """Fusion methods for combining multiple prefetch results."""

    @final
    class Dbsf(Fusion):
        """DBSF (Distribution-Based Score Fusion)."""

        __match_args__: Final = ()

        def __new__(): ...

    @final
    class Rrf(Fusion):
        """RRF (Reciprocal Rank Fusion) with given parameters.

        Args:
            k: The RRF k parameter.
            weights: Optional weights for each prefetch source.
                     Higher weight gives more influence on the final ranking.
                     If not specified, all prefetches are weighted equally.

        Examples:
            # Basic RRF with k=2
            Fusion.Rrf(k=2)

            # Weighted RRF - first prefetch has 3x weight
            Fusion.Rrf(k=2, weights=[3.0, 1.0])"""

        __match_args__: Final = ("k", "weights")

        def __new__(k: int, weights: None | Sequence[float] = None): ...
        @property
        def k() -> int: ...
        @property
        def weights() -> None | list[float]: ...

    def __repr__() -> str: ...

@final
class GeoBoundingBox:
    """A geographic bounding box."""

    def __new__(top_left: GeoPoint, bottom_right: GeoPoint):
        """Create a GeoBoundingBox.

        Args:
            top_left: Top-left corner.
            bottom_right: Bottom-right corner."""

    def __repr__() -> str: ...
    @property
    def bottom_right() -> GeoPoint:
        """Bottom-right corner."""

    @property
    def top_left() -> GeoPoint:
        """Top-left corner."""

@final
class GeoIndexParams:
    """Index parameters for geo fields."""

    def __new__(on_disk: None | bool = None, enable_hnsw: None | bool = None):
        """Create GeoIndexParams.

        Args:
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

@final
class GeoPoint:
    """A geographic point."""

    def __new__(lon: float, lat: float):
        """Create a GeoPoint.

        Args:
            lon: Longitude (-180 to 180).
            lat: Latitude (-90 to 90)."""

    def __repr__() -> str: ...
    @property
    def lat() -> float:
        """Latitude."""

    @property
    def lon() -> float:
        """Longitude."""

@final
class GeoPolygon:
    """A geographic polygon."""

    def __new__(
        exterior: GeoLineString, interiors: None | Sequence[GeoLineString] = None
    ):
        """Create a GeoPolygon.

        Args:
            exterior: Exterior ring points.
            interiors: Optional interior rings (holes)."""

    def __repr__() -> str: ...
    @property
    def exterior() -> GeoLineString:
        """Exterior ring."""

    @property
    def interiors() -> None | list[GeoLineString]:
        """Interior rings (holes)."""

@final
class GeoRadius:
    """A geographic circle."""

    def __new__(center: GeoPoint, radius: float):
        """Create a GeoRadius.

        Args:
            center: Center point.
            radius: Radius in meters."""

    def __repr__() -> str: ...
    @property
    def center() -> GeoPoint:
        """Center point."""

    @property
    def radius() -> float:
        """Radius in meters."""

@final
class HasIdCondition:
    """Check if point ID is in a set."""

    def __new__(point_ids: set[PointId]):
        """Create a HasIdCondition.

        Args:
            point_ids: Set of point IDs."""

    def __repr__() -> str: ...
    @property
    def point_ids() -> set[PointId]:
        """Point IDs."""

@final
class HasVectorCondition:
    """Check if point has a specific vector."""

    def __new__(vector: str):
        """Create a HasVectorCondition.

        Args:
            vector: Vector name."""

    def __repr__() -> str: ...
    @property
    def vector() -> str:
        """Vector name."""

@final
class HnswIndexConfig:
    """Configuration for HNSW index."""

    def __new__(
        m: int,
        ef_construct: int,
        full_scan_threshold: int,
        max_indexing_threads: int = 0,
        on_disk: None | bool = None,
        payload_m: None | int = None,
        inline_storage: None | bool = None,
    ):
        """Create an HnswIndexConfig.

        Args:
            m: Number of edges per node.
            ef_construct: Number of candidates during index construction.
            full_scan_threshold: Threshold for full scan.
            max_indexing_threads: Max threads for HNSW indexing (0 = auto).
            on_disk: Whether to store on disk.
            payload_m: Payload index m value.
            inline_storage: Whether to use inline storage."""

    def __repr__() -> str: ...
    @property
    def ef_construct() -> int:
        """ef_construct value."""

    @property
    def full_scan_threshold() -> int:
        """Full scan threshold."""

    @property
    def inline_storage() -> None | bool:
        """Inline storage flag."""

    @property
    def m() -> int:
        """Number of edges per node."""

    @property
    def max_indexing_threads() -> int:
        """Max indexing threads (0 = auto)."""

    @property
    def on_disk() -> None | bool:
        """On-disk flag."""

    @property
    def payload_m() -> None | int:
        """Payload m value."""

@final
class IdfParams:
    """Population over which sparse vector IDF statistics are computed - the IDF corpus.

    Only applicable to sparse vectors with the IDF modifier enabled."""

    def __new__(corpus: Filter | None = None):
        """Create IdfParams.

        Args:
            corpus: Filter defining the corpus: IDF statistics are computed over
                the points matching this filter. If None, statistics are
                collection-wide (global)."""

    def __repr__() -> str: ...
    @property
    def corpus() -> Filter | None:
        """Corpus filter, None for global statistics."""

@final
class IntegerIndexParams:
    """Index parameters for integer fields."""

    def __new__(
        lookup: None | bool = None,
        range: None | bool = None,
        is_principal: None | bool = None,
        on_disk: None | bool = None,
        enable_hnsw: None | bool = None,
    ):
        """Create IntegerIndexParams.

        Args:
            lookup: Enable exact match filtering.
            range: Enable range filtering.
            is_principal: Whether this field is a principal identifier.
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def is_principal() -> None | bool:
        """Whether this field is a principal identifier."""

    @property
    def lookup() -> None | bool:
        """Enable exact match filtering."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

    @property
    def range() -> None | bool:
        """Enable range filtering."""

@final
class IsEmptyCondition:
    """Check if a field is empty."""

    def __new__(key: JsonPath):
        """Create an IsEmptyCondition.

        Args:
            key: Payload field path."""

    def __repr__() -> str: ...
    @property
    def key() -> JsonPath:
        """Field key."""

@final
class IsNullCondition:
    """Check if a field is null."""

    def __new__(key: JsonPath):
        """Create an IsNullCondition.

        Args:
            key: Payload field path."""

    def __repr__() -> str: ...
    @property
    def key() -> JsonPath:
        """Field key."""

@final
class KeywordIndexParams:
    """Index parameters for keyword fields."""

    def __new__(
        is_tenant: None | bool = None,
        on_disk: None | bool = None,
        enable_hnsw: None | bool = None,
        prefix: None | bool = None,
    ):
        """Create KeywordIndexParams.

        Args:
            is_tenant: Whether this field is used for tenant separation.
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field.
            prefix: Whether to enable prefix matching for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def is_tenant() -> None | bool:
        """Whether this field is used for tenant separation."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

    @property
    def prefix() -> None | bool:
        """Whether prefix matching is enabled."""

@final
class Language:
    """Predefined stopword languages."""

    Arabic: Final[Language]
    Azerbaijani: Final[Language]
    Basque: Final[Language]
    Bengali: Final[Language]
    Catalan: Final[Language]
    Chinese: Final[Language]
    Danish: Final[Language]
    Dutch: Final[Language]
    English: Final[Language]
    Finnish: Final[Language]
    French: Final[Language]
    German: Final[Language]
    Greek: Final[Language]
    Hebrew: Final[Language]
    Hinglish: Final[Language]
    Hungarian: Final[Language]
    Indonesian: Final[Language]
    Italian: Final[Language]
    Japanese: Final[Language]
    Kazakh: Final[Language]
    Nepali: Final[Language]
    Norwegian: Final[Language]
    Portuguese: Final[Language]
    Romanian: Final[Language]
    Russian: Final[Language]
    Slovene: Final[Language]
    Spanish: Final[Language]
    Swedish: Final[Language]
    Tajik: Final[Language]
    Turkish: Final[Language]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class MatchAny:
    """Match any of the values."""

    def __new__(any: list[int] | list[str]):
        """Create a MatchAny.

        Args:
            any: List of values to match any of."""

    def __repr__() -> str: ...
    @property
    def value() -> list[int] | list[str]:
        """Values."""

@final
class MatchExcept:
    """Match any value except these."""

    def __new__(value: list[int] | list[str]):
        """Create a MatchExcept.

        Args:
            value: List of values to exclude."""

    def __repr__() -> str: ...
    @property
    def value() -> list[int] | list[str]:
        """Excluded values."""

@final
class MatchPhrase:
    """Match exact phrase."""

    def __new__(phrase: str):
        """Create a MatchPhrase.

        Args:
            phrase: Phrase to match."""

    def __repr__() -> str: ...
    @property
    def phrase() -> str:
        """Phrase."""

@final
class MatchPrefix:
    """Match keyword values starting with the given prefix."""

    def __new__(prefix: str):
        """Create a MatchPrefix.

        Args:
            prefix: Prefix to match."""

    def __repr__() -> str: ...
    @property
    def prefix() -> str:
        """Prefix."""

@final
class MatchText:
    """Full-text match."""

    def __new__(text: str):
        """Create a MatchText.

        Args:
            text: Text to search for."""

    def __repr__() -> str: ...
    @property
    def text() -> str:
        """Text."""

@final
class MatchTextAny:
    """Match any of the words in text."""

    def __new__(text_any: str):
        """Create a MatchTextAny.

        Args:
            text_any: Space-separated words to match any of."""

    def __repr__() -> str: ...
    @property
    def text_any() -> str:
        """Text."""

@final
class MatchValue:
    """Match exact value."""

    def __new__(value: bool | int | str):
        """Create a MatchValue.

        Args:
            value: Value to match."""

    @property
    def value() -> bool | int | str:
        """Value."""

@final
class MinShould:
    """Minimum number of should conditions that must match."""

    def __new__(conditions: Sequence[ConditionType], min_count: int):
        """Create a MinShould.

        Args:
            conditions: List of conditions.
            min_count: Minimum number that must match."""

    def __repr__() -> str: ...
    @property
    def conditions() -> list[ConditionType]:
        """Conditions."""

    @property
    def min_count() -> int:
        """Minimum count."""

@final
class Mmr:
    """Maximal Marginal Relevance for result diversification."""

    def __new__(
        vector: NamedVector,
        lambda_: float,
        candidates_limit: int,
        using: None | str = None,
    ):
        """Create an MMR query.

        Args:
            vector: Query vector.
            lambda_: Balance between relevance and diversity (0-1).
            candidates_limit: Number of candidates to consider.
            using: Named vector to use."""

    def __repr__() -> str: ...
    @property
    def candidates_limit() -> int:
        """Candidates limit."""

    @property
    def lambda_() -> float:
        """Balance between relevance and diversity."""

    @property
    def using() -> str:
        """Named vector."""

    @property
    def vector() -> NamedVector:
        """Query vector."""

@final
class Modifier:
    """Sparse vector modifiers."""

    Idf: Final[Modifier]
    None_: Final[Modifier]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class MultiVectorComparator:
    """Multi-vector comparison methods."""

    MaxSim: Final[MultiVectorComparator]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class MultiVectorConfig:
    """Configuration for multi-vector storage."""

    def __new__(comparator: MultiVectorComparator):
        """Create a MultiVectorConfig.

        Args:
            comparator: Multi-vector comparator."""

    def __repr__() -> str: ...
    @property
    def comparator() -> MultiVectorComparator:
        """Comparator."""

@final
class NaiveFeedbackStrategy:
    """Coefficients for naive feedback query."""

    def __new__(a: float, b: float, c: float):
        """Create NaiveFeedbackStrategy coefficients.

        Args:
            a: Coefficient a.
            b: Coefficient b.
            c: Coefficient c."""

    def __repr__() -> str: ...
    @property
    def a() -> float:
        """Coefficient a."""

    @property
    def b() -> float:
        """Coefficient b."""

    @property
    def c() -> float:
        """Coefficient c."""

@final
class NestedCondition:
    """Condition on nested objects."""

    def __new__(key: JsonPath, filter: Filter):
        """Create a NestedCondition.

        Args:
            key: Path to nested array.
            filter: Filter to apply to nested objects."""

    def __repr__() -> str: ...
    @property
    def filter() -> Filter:
        """Nested filter."""

    @property
    def key() -> JsonPath:
        """Nested field key."""

@final
class OrderBy:
    """Order results by a payload field."""

    def __new__(
        key: JsonPath,
        direction: Direction | None = None,
        start_from: None | StartFromType = None,
    ):
        """Create an OrderBy.

        Args:
            key: Payload field path.
            direction: Sort direction.
            start_from: Starting value."""

    def __repr__() -> str: ...
    @property
    def direction() -> Direction | None:
        """Sort direction."""

    @property
    def key() -> JsonPath:
        """Field key."""

    @property
    def start_from() -> None | StartFromType:
        """Starting value."""

@final
class PayloadIndexInfo:
    """Information about a payload index."""

    @property
    def data_type() -> PayloadSchemaType:
        """Data type."""

    @property
    def params() -> None | PayloadSchemaParams:
        """Index parameters."""

    @property
    def points() -> int:
        """Number of points with this field."""

@final
class PayloadSchemaType:
    """Payload field schema types."""

    Bool: Final[PayloadSchemaType]
    Datetime: Final[PayloadSchemaType]
    Float: Final[PayloadSchemaType]
    Geo: Final[PayloadSchemaType]
    Integer: Final[PayloadSchemaType]
    Keyword: Final[PayloadSchemaType]
    Text: Final[PayloadSchemaType]
    Uuid: Final[PayloadSchemaType]

    def __int__() -> int: ...
    def __repr__() -> str: ...

class PayloadSelector:
    """Select specific payload fields."""

    @final
    class Exclude(PayloadSelector):
        """Exclude specified fields."""

        __match_args__: Final = ("keys",)

        def __new__(keys: Sequence[JsonPath]): ...
        @property
        def keys() -> list[JsonPath]: ...

    @final
    class Include(PayloadSelector):
        """Include only specified fields."""

        __match_args__: Final = ("keys",)

        def __new__(keys: Sequence[JsonPath]): ...
        @property
        def keys() -> list[JsonPath]: ...

@final
class PlainIndexConfig:
    """Configuration for plain (brute-force) index."""

    def __new__():
        """Create a PlainIndexConfig."""

    def __repr__() -> str: ...

@final
class Point:
    """A point with ID, vector(s), and optional payload."""

    def __new__(id: PointId, vector: Vector, payload: None | Payload = None):
        """Create a Point.

        Args:
            id: Point ID (integer or UUID).
            vector: Vector data.
            payload: Optional payload dictionary."""

    def __repr__() -> str: ...
    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def payload() -> None | Payload:
        """Payload."""

    @property
    def vector() -> Vector:
        """Vector data."""

@final
class PointVectors:
    """Point ID with associated vectors for update operations."""

    def __new__(id: PointId, vector: Vector):
        """Create a PointVectors.

        Args:
            id: Point ID.
            vector: Vector data."""

    def __repr__() -> str: ...
    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def vector() -> Vector:
        """Vector data."""

@final
class Prefetch:
    """A prefetch stage for multi-stage queries."""

    def __new__(
        limit: int,
        query: None | ScoringQueryType = None,
        prefetches: None | Sequence[Prefetch] = None,
        params: None | SearchParams = None,
        filter: Filter | None = None,
        score_threshold: None | float = None,
    ):
        """Create a Prefetch stage.

        Args:
            limit: Maximum number of results for this stage.
            query: Scoring query.
            prefetches: Nested prefetch stages.
            params: Search parameters.
            filter: Filter conditions.
            score_threshold: Minimum score threshold."""

    def __repr__() -> str: ...
    @property
    def filter() -> Filter | None:
        """Filter."""

    @property
    def limit() -> int:
        """Result limit."""

    @property
    def params() -> None | SearchParams:
        """Search parameters."""

    @property
    def prefetches() -> list[Prefetch]:
        """Nested prefetch stages."""

    @property
    def query() -> None | ScoringQueryType:
        """Scoring query."""

    @property
    def score_threshold() -> None | float:
        """Score threshold."""

@final
class ProductQuantizationConfig:
    """Configuration for product quantization."""

    def __new__(compression: CompressionRatio, always_ram: None | bool = None):
        """Create a ProductQuantizationConfig.

        Args:
            compression: Compression ratio.
            always_ram: Whether to keep in RAM."""

    def __repr__() -> str: ...
    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def compression() -> CompressionRatio:
        """Compression ratio."""

@final
class QuantizationSearchParams:
    """Parameters for quantization during search."""

    def __new__(
        ignore: bool = False,
        rescore: None | bool = None,
        oversampling: None | float = None,
    ):
        """Create QuantizationSearchParams.

        Args:
            ignore: Whether to ignore quantization.
            rescore: Whether to rescore with original vectors.
            oversampling: Oversampling factor."""

    def __repr__() -> str: ...
    @property
    def ignore() -> bool:
        """Ignore quantization flag."""

    @property
    def oversampling() -> None | float:
        """Oversampling factor."""

    @property
    def rescore() -> None | bool:
        """Rescore flag."""

class Query:
    """Query types for vector search."""

    @final
    class Context(Query):
        """Create a context query."""

        __match_args__: Final = ("query", "using")

        def __new__(query: ContextQuery, using: None | str = None): ...
        @property
        def query() -> ContextQuery: ...
        @property
        def using() -> None | str: ...

    @final
    class Discover(Query):
        """Create a discover query."""

        __match_args__: Final = ("query", "using")

        def __new__(query: DiscoverQuery, using: None | str = None): ...
        @property
        def query() -> DiscoverQuery: ...
        @property
        def using() -> None | str: ...

    @final
    class FeedbackNaive(Query):
        """Create a feedback naive query."""

        __match_args__: Final = ("query", "using")

        def __new__(query: FeedbackNaiveQuery, using: None | str = None): ...
        @property
        def query() -> FeedbackNaiveQuery: ...
        @property
        def using() -> None | str: ...

    @final
    class Nearest(Query):
        """Create a nearest neighbor query."""

        __match_args__: Final = ("query", "using")

        def __new__(query: NamedVector, using: None | str = None): ...
        @property
        def query() -> NamedVector: ...
        @property
        def using() -> None | str: ...

    @final
    class RecommendBestScore(Query):
        """Create a recommend query using best score."""

        __match_args__: Final = ("query", "using")

        def __new__(query: RecommendQuery, using: None | str = None): ...
        @property
        def query() -> RecommendQuery: ...
        @property
        def using() -> None | str: ...

    @final
    class RecommendSumScores(Query):
        """Create a recommend query using sum of scores."""

        __match_args__: Final = ("query", "using")

        def __new__(query: RecommendQuery, using: None | str = None): ...
        @property
        def query() -> RecommendQuery: ...
        @property
        def using() -> None | str: ...

    def __repr__() -> str: ...

@final
class QueryBatchRequest:
    """Queries executed together as one planned batch."""

    def __new__(queries: Sequence[QueryRequest]):
        """Create a batch of queries, returning results in the same order."""

    def __repr__() -> str: ...
    @property
    def queries() -> list[QueryRequest]: ...

@final
class QueryRequest:
    """Request for query operation."""

    def __new__(
        limit: int,
        offset: None | int = None,
        query: None | ScoringQueryType = None,
        prefetches: None | Sequence[Prefetch] = None,
        with_vector: None | WithVectorType = None,
        with_payload: None | WithPayloadType = None,
        filter: Filter | None = None,
        score_threshold: None | float = None,
        params: None | SearchParams = None,
    ):
        """Create a QueryRequest.

        Args:
            limit: Maximum number of results.
            offset: Number of results to skip.
            query: Scoring query (vector, fusion, order_by, etc.).
            prefetches: Prefetch stages for multi-stage queries.
            with_vector: Whether to include vectors.
            with_payload: Whether to include payload.
            filter: Filter conditions.
            score_threshold: Minimum score threshold.
            params: Search parameters."""

    def __repr__() -> str: ...
    @property
    def filter() -> Filter | None:
        """Filter."""

    @property
    def limit() -> int:
        """Result limit."""

    @property
    def offset() -> int:
        """Result offset."""

    @property
    def params() -> None | SearchParams:
        """Search parameters."""

    @property
    def prefetches() -> list[Prefetch]:
        """Prefetch stages."""

    @property
    def query() -> None | ScoringQueryType:
        """Scoring query."""

    @property
    def score_threshold() -> None | float:
        """Score threshold."""

    @property
    def with_payload() -> WithPayloadType:
        """With payload flag."""

    @property
    def with_vector() -> WithVectorType:
        """With vector flag."""

@final
class RangeDateTime:
    """Range condition for datetime values."""

    def __new__(
        gte: None | str = None,
        gt: None | str = None,
        lte: None | str = None,
        lt: None | str = None,
    ):
        """Create a RangeDateTime.

        Args:
            gte: Greater than or equal (ISO 8601 string).
            gt: Greater than (ISO 8601 string).
            lte: Less than or equal (ISO 8601 string).
            lt: Less than (ISO 8601 string)."""

    @property
    def gt() -> None | str:
        """Greater than."""

    @property
    def gte() -> None | str:
        """Greater than or equal."""

    @property
    def lt() -> None | str:
        """Less than."""

    @property
    def lte() -> None | str:
        """Less than or equal."""

@final
class RangeFloat:
    """Range condition for float values."""

    def __new__(
        gte: None | float = None,
        gt: None | float = None,
        lte: None | float = None,
        lt: None | float = None,
    ):
        """Create a RangeFloat.

        Args:
            gte: Greater than or equal.
            gt: Greater than.
            lte: Less than or equal.
            lt: Less than."""

    @property
    def gt() -> None | float:
        """Greater than."""

    @property
    def gte() -> None | float:
        """Greater than or equal."""

    @property
    def lt() -> None | float:
        """Less than."""

    @property
    def lte() -> None | float:
        """Less than or equal."""

@final
class RecommendQuery:
    """Query for recommendation based on positive and negative examples."""

    def __new__(positives: Sequence[NamedVector], negatives: Sequence[NamedVector]):
        """Create a RecommendQuery.

        Args:
            positives: Positive example vectors.
            negatives: Negative example vectors."""

    def __repr__() -> str: ...
    @property
    def negatives() -> list[NamedVector]:
        """Negative examples."""

    @property
    def positives() -> list[NamedVector]:
        """Positive examples."""

@final
class Record:
    """A retrieved point record."""

    def __repr__() -> str: ...
    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def order_value() -> None | float | int:
        """Order value for order_by queries."""

    @property
    def payload() -> None | Payload:
        """Payload (if requested)."""

    @property
    def vector() -> None | Vector:
        """Vector data (if requested)."""

@final
class Sample:
    """Sampling methods."""

    Random: Final[Sample]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class ScalarQuantizationConfig:
    """Configuration for scalar quantization."""

    def __new__(
        type: ScalarType, quantile: None | float = None, always_ram: None | bool = None
    ):
        """Create a ScalarQuantizationConfig.

        Args:
            type: Scalar type (e.g., Int8).
            quantile: Quantile for normalization.
            always_ram: Whether to keep in RAM."""

    def __repr__() -> str: ...
    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def quantile() -> None | float:
        """Quantile."""

    @property
    def type() -> ScalarType:
        """Scalar type."""

@final
class ScalarType:
    """Scalar quantization types."""

    Int8: Final[ScalarType]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class ScoredPoint:
    """A point with a similarity score."""

    def __repr__() -> str: ...
    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def order_value() -> None | float | int:
        """Order value for order_by queries."""

    @property
    def payload() -> None | Payload:
        """Payload (if requested)."""

    @property
    def score() -> float:
        """Similarity score."""

    @property
    def vector() -> None | Vector:
        """Vector data (if requested)."""

    @property
    def version() -> int:
        """Point version."""

@final
class ScrollRequest:
    """Request for scroll operation."""

    def __new__(
        offset: None | PointId = None,
        limit: None | int = None,
        filter: Filter | None = None,
        with_payload: None | WithPayloadType = None,
        with_vector: None | WithVectorType = None,
        order_by: None | OrderBy = None,
    ):
        """Create a ScrollRequest.

        Args:
            offset: Starting point ID.
            limit: Maximum number of results.
            filter: Filter conditions.
            with_payload: Whether to include payload.
            with_vector: Whether to include vectors.
            order_by: Order by configuration."""

    def __repr__() -> str: ...
    @property
    def filter() -> Filter | None:
        """Filter."""

    @property
    def limit() -> None | int:
        """Result limit."""

    @property
    def offset() -> None | PointId:
        """Offset point ID."""

    @property
    def order_by() -> None | OrderBy:
        """Order by configuration."""

    @property
    def with_payload() -> None | WithPayloadType:
        """With payload flag."""

    @property
    def with_vector() -> WithVectorType:
        """With vector flag."""

@final
class SearchParams:
    """Parameters for search operations."""

    def __new__(
        hnsw_ef: None | int = None,
        exact: bool = False,
        quantization: None | QuantizationSearchParams = None,
        indexed_only: bool = False,
        acorn: AcornSearchParams | None = None,
        idf: IdfParams | None = None,
    ):
        """Create SearchParams.

        Args:
            hnsw_ef: ef parameter for HNSW search.
            exact: Whether to use exact search.
            quantization: Quantization search parameters.
            indexed_only: Whether to search only indexed vectors.
            acorn: Acorn search parameters.
            idf: Population over which sparse IDF statistics are computed."""

    def __repr__() -> str: ...
    @property
    def acorn() -> AcornSearchParams | None:
        """Acorn parameters."""

    @property
    def exact() -> bool:
        """Exact search flag."""

    @property
    def hnsw_ef() -> None | int:
        """HNSW ef parameter."""

    @property
    def idf() -> IdfParams | None:
        """IDF scope parameters."""

    @property
    def indexed_only() -> bool:
        """Indexed only flag."""

    @property
    def quantization() -> None | QuantizationSearchParams:
        """Quantization parameters."""

@final
class SearchRequest:
    """Request for search operation."""

    def __new__(
        query: Query,
        limit: int,
        offset: None | int = None,
        filter: Filter | None = None,
        params: None | SearchParams = None,
        with_vector: None | WithVectorType = None,
        with_payload: None | WithPayloadType = None,
        score_threshold: None | float = None,
    ):
        """Create a SearchRequest.

        Args:
            query: Query (vector-based).
            limit: Maximum number of results.
            offset: Number of results to skip.
            filter: Filter conditions.
            params: Search parameters.
            with_vector: Whether to include vectors.
            with_payload: Whether to include payload.
            score_threshold: Minimum score threshold."""

    def __repr__() -> str: ...
    @property
    def filter() -> Filter | None:
        """Filter."""

    @property
    def limit() -> int:
        """Result limit."""

    @property
    def offset() -> int:
        """Result offset."""

    @property
    def params() -> None | SearchParams:
        """Search parameters."""

    @property
    def query() -> Query:
        """Query."""

    @property
    def score_threshold() -> None | float:
        """Score threshold."""

    @property
    def with_payload() -> None | WithPayloadType:
        """With payload flag."""

    @property
    def with_vector() -> None | WithVectorType:
        """With vector flag."""

@final
class ShardInfo:
    """Information about a shard."""

    def __repr__() -> str: ...
    @property
    def indexed_vectors_count() -> int:
        """Number of indexed vectors."""

    @property
    def payload_schema() -> dict[JsonPath, PayloadIndexInfo]:
        """Payload schema information."""

    @property
    def points_count() -> int:
        """Number of points."""

    @property
    def segments_count() -> int:
        """Number of segments."""

@final
class SliceCondition:
    def __new__(total: int, index: int): ...
    def __repr__() -> str: ...
    @property
    def index() -> int: ...
    @property
    def total() -> int: ...

@final
class SnowballLanguage:
    """Snowball stemmer languages."""

    Arabic: Final[SnowballLanguage]
    Armenian: Final[SnowballLanguage]
    Danish: Final[SnowballLanguage]
    Dutch: Final[SnowballLanguage]
    English: Final[SnowballLanguage]
    Finnish: Final[SnowballLanguage]
    French: Final[SnowballLanguage]
    German: Final[SnowballLanguage]
    Greek: Final[SnowballLanguage]
    Hungarian: Final[SnowballLanguage]
    Italian: Final[SnowballLanguage]
    Norwegian: Final[SnowballLanguage]
    Portuguese: Final[SnowballLanguage]
    Romanian: Final[SnowballLanguage]
    Russian: Final[SnowballLanguage]
    Spanish: Final[SnowballLanguage]
    Swedish: Final[SnowballLanguage]
    Tamil: Final[SnowballLanguage]
    Turkish: Final[SnowballLanguage]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class SnowballParams:
    """Snowball stemming algorithm parameters."""

    def __new__(language: SnowballLanguage):
        """Create SnowballParams.

        Args:
            language: Snowball language."""

    @property
    def language() -> SnowballLanguage:
        """Snowball language."""

@final
class SparseVector:
    """A sparse vector representation."""

    def __new__(indices: Sequence[int], values: Sequence[float]):
        """Create a SparseVector.

        Args:
            indices: Non-zero dimension indices.
            values: Values at the non-zero dimensions."""

    def __repr__() -> str: ...
    @property
    def indices() -> list[int]:
        """Non-zero dimension indices."""

    @property
    def values() -> list[float]:
        """Values at non-zero dimensions."""

@final
class StopwordsSet:
    """Custom stopwords set."""

    def __new__(languages: None | set[Language] = None, custom: None | set[str] = None):
        """Create a StopwordsSet.

        Args:
            languages: Predefined language stopwords to include.
            custom: Custom stopwords to add."""

    @property
    def custom() -> None | set[str]:
        """Custom stopwords."""

    @property
    def languages() -> None | set[Language]:
        """Predefined language stopwords."""

@final
class TextIndexParams:
    """Index parameters for text fields."""

    def __new__(
        tokenizer: None | TokenizerType = None,
        min_token_len: None | int = None,
        max_token_len: None | int = None,
        lowercase: None | bool = None,
        ascii_folding: None | bool = None,
        phrase_matching: None | bool = None,
        stopwords: None | Stopwords = None,
        on_disk: None | bool = None,
        stemmer: None | StemmingAlgorithm = None,
        enable_hnsw: None | bool = None,
    ):
        """Create TextIndexParams.

        Args:
            tokenizer: Tokenizer type.
            min_token_len: Minimum token length.
            max_token_len: Maximum token length.
            lowercase: Convert to lowercase.
            ascii_folding: Apply ASCII folding.
            phrase_matching: Enable phrase matching.
            stopwords: Stopwords configuration.
            on_disk: Whether to store index on disk.
            stemmer: Stemming algorithm.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def ascii_folding() -> None | bool:
        """Apply ASCII folding."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def lowercase() -> None | bool:
        """Convert to lowercase."""

    @property
    def max_token_len() -> None | int:
        """Maximum token length."""

    @property
    def min_token_len() -> None | int:
        """Minimum token length."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

    @property
    def phrase_matching() -> None | bool:
        """Enable phrase matching."""

    @property
    def stemmer() -> None | StemmingAlgorithm:
        """Stemming algorithm."""

    @property
    def stopwords() -> None | Stopwords:
        """Stopwords configuration."""

    @property
    def tokenizer() -> TokenizerType:
        """Tokenizer type."""

@final
class TokenizerType:
    """Text tokenizer types."""

    Multilingual: Final[TokenizerType]
    Prefix: Final[TokenizerType]
    Whitespace: Final[TokenizerType]
    Word: Final[TokenizerType]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class TurboQuantBitSize:
    """TurboQuant bit size for compressed codes."""

    Bits1: Final[TurboQuantBitSize]
    Bits1_5: Final[TurboQuantBitSize]
    Bits2: Final[TurboQuantBitSize]
    Bits4: Final[TurboQuantBitSize]

    def __int__() -> int: ...
    def __repr__() -> str: ...

@final
class TurboQuantQuantizationConfig:
    """Configuration for TurboQuant quantization."""

    def __new__(always_ram: None | bool = None, bits: None | TurboQuantBitSize = None):
        """Create a TurboQuantQuantizationConfig.

        Args:
            always_ram: Whether to keep in RAM.
            bits: Bit size used for compressed codes."""

    def __repr__() -> str: ...
    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def bits() -> None | TurboQuantBitSize:
        """Bit size."""

@final
class UpdateMode:
    """Defines the mode of the upsert operation."""

    InsertOnly: Final[UpdateMode]
    """
    Only insert new points, do not update existing points.
    """
    UpdateOnly: Final[UpdateMode]
    """
    Only update existing points, do not insert new points.
    """
    Upsert: Final[UpdateMode]
    """
    Default mode - insert new points, update existing points.
    """

    def __eq__(value: object) -> bool: ...
    def __int__() -> int: ...
    def __ne__(value: object) -> bool: ...
    def __repr__() -> str: ...

@final
class UpdateOperation:
    """Operations for updating shard data."""

    @staticmethod
    def clear_payload(point_ids: Sequence[PointId]) -> UpdateOperation:
        """Clear all payload from points.

        Args:
            point_ids: Point IDs."""

    @staticmethod
    def clear_payload_by_filter(filter: Filter) -> UpdateOperation:
        """Clear all payload from points matching a filter.

        Args:
            filter: Filter for points."""

    @staticmethod
    def create_dense_vector(
        vector_name: str,
        size: int,
        distance: Distance,
        multivector_config: MultiVectorConfig | None = None,
        datatype: None | VectorStorageDatatype = None,
    ) -> UpdateOperation:
        """Create a new dense named vector on the collection.

        Args:
            vector_name: Name for the new vector.
            size: Dimensionality of the vectors.
            distance: Distance function (Cosine, Euclid, Dot, Manhattan).
            multivector_config: Optional multi-vector configuration (e.g., for ColBERT).
            datatype: Optional element storage type (Float32, Float16, Uint8)."""

    @staticmethod
    def create_field_index(
        field_name: JsonPath, schema: PayloadFieldSchema
    ) -> UpdateOperation:
        """Create an index on a payload field.

        Args:
            field_name: Path to the payload field.
            schema: Schema type or index parameters for the field."""

    @staticmethod
    def create_sparse_vector(
        vector_name: str,
        modifier: Modifier | None = None,
        datatype: None | VectorStorageDatatype = None,
    ) -> UpdateOperation:
        """Create a new sparse named vector on the collection.

        Args:
            vector_name: Name for the new sparse vector.
            modifier: Optional value modifier (e.g., Modifier.Idf).
            datatype: Optional datatype for storing weights in the index."""

    @staticmethod
    def delete_field_index(field_name: JsonPath) -> UpdateOperation:
        """Delete an index from a payload field.

        Args:
            field_name: Path to the payload field."""

    @staticmethod
    def delete_payload(
        point_ids: Sequence[PointId], keys: Sequence[JsonPath]
    ) -> UpdateOperation:
        """Delete payload fields from points.

        Args:
            point_ids: Point IDs.
            keys: Payload field keys to delete."""

    @staticmethod
    def delete_payload_by_filter(
        filter: Filter, keys: Sequence[JsonPath]
    ) -> UpdateOperation:
        """Delete payload fields from points matching a filter.

        Args:
            filter: Filter for points.
            keys: Payload field keys to delete."""

    @staticmethod
    def delete_points(point_ids: Sequence[PointId]) -> UpdateOperation:
        """Delete points by ID.

        Args:
            point_ids: IDs of points to delete."""

    @staticmethod
    def delete_points_by_filter(filter: Filter) -> UpdateOperation:
        """Delete points matching a filter.

        Args:
            filter: Filter for points to delete."""

    @staticmethod
    def delete_vector_name(vector_name: str) -> UpdateOperation:
        """Delete a named vector from the collection.

        Args:
            vector_name: Name of the vector to delete."""

    @staticmethod
    def delete_vectors(
        point_ids: Sequence[PointId], vector_names: Sequence[str]
    ) -> UpdateOperation:
        """Delete specific vectors from points.

        Args:
            point_ids: Point IDs.
            vector_names: Names of vectors to delete."""

    @staticmethod
    def delete_vectors_by_filter(
        filter: Filter, vector_names: Sequence[str]
    ) -> UpdateOperation:
        """Delete vectors from points matching a filter.

        Args:
            filter: Filter for points.
            vector_names: Names of vectors to delete."""

    @staticmethod
    def overwrite_payload(
        point_ids: Sequence[PointId], payload: Payload, key: JsonPath | None = None
    ) -> UpdateOperation:
        """Overwrite entire payload on points.

        Args:
            point_ids: Point IDs.
            payload: New payload.
            key: Optional nested key path."""

    @staticmethod
    def overwrite_payload_by_filter(
        filter: Filter, payload: Payload, key: JsonPath | None = None
    ) -> UpdateOperation:
        """Overwrite payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: New payload.
            key: Optional nested key path."""

    @staticmethod
    def set_payload(
        point_ids: Sequence[PointId], payload: Payload, key: JsonPath | None = None
    ) -> UpdateOperation:
        """Set payload fields on points.

        Args:
            point_ids: Point IDs.
            payload: Payload to set.
            key: Optional nested key path."""

    @staticmethod
    def set_payload_by_filter(
        filter: Filter, payload: Payload, key: JsonPath | None = None
    ) -> UpdateOperation:
        """Set payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: Payload to set.
            key: Optional nested key path."""

    @staticmethod
    def update_vectors(
        point_vectors: Sequence[PointVectors], condition: Filter | None = None
    ) -> UpdateOperation:
        """Update vectors of existing points.

        Args:
            point_vectors: Point IDs with new vectors.
            condition: Optional filter condition."""

    @staticmethod
    def upsert_points(
        points: Sequence[Point],
        condition: Filter | None = None,
        update_mode: None | UpdateMode = None,
    ) -> UpdateOperation:
        """Insert or update points.

        Args:
            points: Points to upsert.
            condition: Optional condition for conditional upsert.
            update_mode: Optional mode of the upsert operation:
                - UpdateMode.Upsert (default): insert new points, update existing points
                - UpdateMode.InsertOnly: only insert new points, do not update existing points
                - UpdateMode.UpdateOnly: only update existing points, do not insert new points
        """

@final
class UuidIndexParams:
    """Index parameters for UUID fields."""

    def __new__(
        is_tenant: None | bool = None,
        on_disk: None | bool = None,
        enable_hnsw: None | bool = None,
    ):
        """Create UuidIndexParams.

        Args:
            is_tenant: Whether this field is used for tenant separation.
            on_disk: Whether to store index on disk.
            enable_hnsw: Whether to enable HNSW index for this field."""

    @property
    def enable_hnsw() -> None | bool:
        """Whether to enable HNSW index."""

    @property
    def is_tenant() -> None | bool:
        """Whether this field is used for tenant separation."""

    @property
    def on_disk() -> None | bool:
        """Whether to store index on disk."""

@final
class ValuesCount:
    """Condition on count of values in array field."""

    def __new__(
        lt: None | int = None,
        gt: None | int = None,
        lte: None | int = None,
        gte: None | int = None,
    ):
        """Create a ValuesCount.

        Args:
            lt: Less than.
            gt: Greater than.
            lte: Less than or equal.
            gte: Greater than or equal."""

    def __repr__() -> str: ...
    @property
    def gt() -> None | int:
        """Greater than."""

    @property
    def gte() -> None | int:
        """Greater than or equal."""

    @property
    def lt() -> None | int:
        """Less than."""

    @property
    def lte() -> None | int:
        """Less than or equal."""

@final
class VectorStorageDatatype:
    """Vector storage data types."""

    Float16: Final[VectorStorageDatatype]
    Float32: Final[VectorStorageDatatype]
    Turbo4: Final[VectorStorageDatatype]
    Uint8: Final[VectorStorageDatatype]

    def __int__() -> int: ...
    def __repr__() -> str: ...
