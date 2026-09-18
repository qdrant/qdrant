ConditionType: TypeAlias = (
    FieldCondition
    | Filter
    | HasIdCondition
    | HasVectorCondition
    | IsEmptyCondition
    | IsNullCondition
    | NestedCondition
)
ExpressionType: TypeAlias = Expression
IndexType: TypeAlias = HnswIndexConfig | PlainIndexConfig
JsonPath: TypeAlias = str
MatchType: TypeAlias = (
    MatchAny
    | MatchExcept
    | MatchPhrase
    | MatchPrefix
    | MatchSubstring
    | MatchText
    | MatchTextAny
    | MatchValue
)
NamedVector: TypeAlias = SparseVector | list[float] | list[list[float]]
Payload: TypeAlias = dict[str, Any]
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
RangeType: TypeAlias = RangeDateTime | RangeFloat
ScoringQueryType: TypeAlias = Formula | Fusion | Mmr | OrderBy | Query | Sample
StartFromType: TypeAlias = float | int | str
StemmingAlgorithm: TypeAlias = DisabledStemmer | SnowballParams
Stopwords: TypeAlias = Language | StopwordsSet
Vector: TypeAlias = dict[str, NamedVector] | list[float] | list[list[float]]
WithPayloadType: TypeAlias = PayloadSelector | bool | list[str]
WithVectorType: TypeAlias = bool | list[str]

class AcornSearchParams:
    """Parameters for Acorn filtered search."""

    def __new__(enable: bool = False, max_selectivity: None | float = None):
        """Create AcornSearchParams.

        Args:
            enable: Whether to enable Acorn.
            max_selectivity: Maximum filter selectivity for Acorn."""

    @property
    def enable() -> bool:
        """Enable flag."""

    @property
    def max_selectivity() -> None | float:
        """Maximum selectivity."""

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

    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def encoding() -> BinaryQuantizationEncoding | None:
        """Encoding."""

    @property
    def query_encoding() -> BinaryQuantizationQueryEncoding | None:
        """Query encoding."""

class BinaryQuantizationEncoding(Enum):
    """Binary quantization encoding types."""

    OneAndHalfBits: Final[BinaryQuantizationEncoding]
    OneBit: Final[BinaryQuantizationEncoding]
    TwoBits: Final[BinaryQuantizationEncoding]

class BinaryQuantizationQueryEncoding(Enum):
    """Binary quantization query encoding types."""

    Binary: Final[BinaryQuantizationQueryEncoding]
    Default: Final[BinaryQuantizationQueryEncoding]
    Scalar4Bits: Final[BinaryQuantizationQueryEncoding]
    Scalar8Bits: Final[BinaryQuantizationQueryEncoding]

class Bm25:
    """BM25 sparse-vector embedding model. No qdrant server / inference service required."""

    def __new__(config: Bm25Config | None = None):
        """Create a Bm25 model with the given configuration (defaults if `None`).

        Raises `ValueError` for invalid configuration: unsupported `language`,
        non-positive `avg_len`, `b` outside `[0.0, 1.0]`, or negative `k`."""

    def embed_document(text: str) -> SparseVector:
        """Embed `text` as an indexed document: term-frequency weights with
        `(k, b, avg_len)` from the model config."""

    def embed_query(text: str) -> SparseVector:
        """Embed `text` as a search query: each unique token gets weight 1.0."""

class Bm25Config:
    """Configuration for an edge-side BM25 model.

    JSON shape mirrors the Qdrant REST/gRPC `Bm25Config` so configs are
    portable between cloud and edge. Defaults match standard BM25
    (k=1.2, b=0.75, avg_len=256) and English-language tokenization."""

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

class CompressionRatio(Enum):
    """Product quantization compression ratios."""

    X16: Final[CompressionRatio]
    X32: Final[CompressionRatio]
    X4: Final[CompressionRatio]
    X64: Final[CompressionRatio]
    X8: Final[CompressionRatio]

class ContextPair:
    """A positive/negative pair for context-based queries."""

    def __new__(positive: NamedVector, negative: NamedVector):
        """Create a ContextPair.

        Args:
            positive: Positive example.
            negative: Negative example."""

    @property
    def negative() -> NamedVector:
        """Negative example."""

    @property
    def positive() -> NamedVector:
        """Positive example."""

class ContextQuery:
    """Query based on context pairs only."""

    def __new__(pairs: list[ContextPair]):
        """Create a ContextQuery.

        Args:
            pairs: Context pairs."""

    @property
    def pairs() -> list[ContextPair]:
        """Context pairs."""

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

class DecayKind(Enum):
    """Decay function kinds for scoring formulas."""

    Exp: Final[DecayKind]
    Gauss: Final[DecayKind]
    Lin: Final[DecayKind]

class Direction(Enum):
    """Sort direction."""

    Asc: Final[Direction]
    Desc: Final[Direction]

class DisabledStemmer:
    """Explicitly disable stemming, overriding the language default.

    Use together with an empty stopword set for language-neutral text
    processing, instead of the deprecated ``language="none"`` hack."""

    def __new__():
        """Create a DisabledStemmer."""

class DiscoverQuery:
    """Query for discovery using a target and context pairs."""

    def __new__(target: NamedVector, pairs: list[ContextPair]):
        """Create a DiscoverQuery.

        Args:
            target: Target vector.
            pairs: Context pairs."""

    @property
    def pairs() -> list[ContextPair]:
        """Context pairs."""

    @property
    def target() -> NamedVector:
        """Target vector."""

class Distance(Enum):
    """Distance metrics for vector comparison."""

    Cosine: Final[Distance]
    Dot: Final[Distance]
    Euclid: Final[Distance]
    Manhattan: Final[Distance]

class EdgeConfig:
    """Configuration for creating a new Qdrant Edge shard."""

    def __new__(
        vectors: EdgeVectorParams | dict[str, EdgeVectorParams] | None = None,
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
    def create(path: str, config: EdgeConfig) -> EdgeShard:
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
    def load(path: str, config: EdgeConfig | None = None) -> EdgeShard:
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
        point_ids: list[PointId],
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
    def unpack_snapshot(snapshot_path: str, target_path: str) -> None:
        """Unpack a snapshot to a target directory.

        Args:
            snapshot_path: Path to the snapshot file.
            target_path: Path to extract the snapshot to."""

    def update(operation: UpdateOperation) -> None:
        """Apply an update operation to the shard.

        Args:
            operation: The update operation to apply."""

    def update_from_snapshot(snapshot_path: str, tmp_dir: None | str = None) -> None:
        """Update the shard from a snapshot.

        Args:
            snapshot_path: Path to the snapshot file.
            tmp_dir: Optional temporary directory for extraction."""

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

class Expression(Enum):
    """Expression types for formulas."""

    @staticmethod
    def Abs(expr: Expression) -> Expression:
        """Create an absolute value expression."""

    @staticmethod
    def Acosh(expr: Expression) -> Expression:
        """Create an inverse hyperbolic cosine expression."""

    @staticmethod
    def Condition(cond: ConditionType) -> Expression:
        """Create a condition expression (returns 1 if true, 0 if false)."""

    @staticmethod
    def Constant(val: float) -> Expression:
        """Create a constant expression."""

    @staticmethod
    def Datetime(date_time: str) -> Expression:
        """Create a datetime constant expression."""

    @staticmethod
    def DatetimeKey(path: JsonPath) -> Expression:
        """Create a datetime field expression."""

    @staticmethod
    def Decay(
        kind: DecayKind,
        x: Expression,
        target: Expression | None = None,
        midpoint: None | float = None,
        scale: None | float = None,
    ) -> Expression:
        """Create a decay expression."""

    @staticmethod
    def Div(
        left: Expression, right: Expression, by_zero_default: None | float = None
    ) -> Expression:
        """Create a division expression."""

    @staticmethod
    def Exp(expr: Expression) -> Expression:
        """Create an exponential expression."""

    @staticmethod
    def GeoDistance(origin: GeoPoint, to: JsonPath) -> Expression:
        """Create a geo distance expression."""

    @staticmethod
    def Ln(expr: Expression) -> Expression:
        """Create a natural log expression."""

    @staticmethod
    def Log10(expr: Expression) -> Expression:
        """Create a log10 expression."""

    @staticmethod
    def Max(exprs: list[Expression]) -> Expression:
        """Create a maximum expression. Requires at least one operand."""

    @staticmethod
    def Min(exprs: list[Expression]) -> Expression:
        """Create a minimum expression. Requires at least one operand."""

    @staticmethod
    def Mult(exprs: list[Expression]) -> Expression:
        """Create a multiplication expression."""

    @staticmethod
    def Neg(expr: Expression) -> Expression:
        """Create a negation expression."""

    @staticmethod
    def Pow(base: Expression, exponent: Expression) -> Expression:
        """Create a power expression."""

    @staticmethod
    def Sqrt(expr: Expression) -> Expression:
        """Create a square root expression."""

    @staticmethod
    def Sum(exprs: list[Expression]) -> Expression:
        """Create a sum expression."""

    @staticmethod
    def Variable(var: str) -> Expression:
        """Create a variable expression."""

class FacetHit:
    """A facet hit with value and count."""

    @property
    def count() -> int:
        """Count of points with this value."""

    @property
    def value() -> bool | int | str:
        """Facet value."""

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
    def key() -> str:
        """Facet key."""

    @property
    def limit() -> int:
        """Result limit."""

class FacetResponse:
    """Response for facet operation."""

    def __iter__() -> Any:
        """Iterate over hits."""

    def __len__() -> int:
        """Number of hits."""

    @property
    def hits() -> list[FacetHit]:
        """Facet hits."""

class FeedbackItem:
    """A feedback item with vector and score."""

    def __new__(vector: NamedVector, score: float):
        """Create a FeedbackItem.

        Args:
            vector: Feedback vector.
            score: Feedback score."""

    @property
    def score() -> float:
        """Feedback score."""

    @property
    def vector() -> NamedVector:
        """Feedback vector."""

class FeedbackNaiveQuery:
    """Query using naive feedback approach."""

    def __new__(
        target: NamedVector,
        feedback: list[FeedbackItem],
        strategy: NaiveFeedbackStrategy,
    ):
        """Create a FeedbackNaiveQuery.

        Args:
            target: Target vector.
            feedback: Feedback items with scores.
            strategy: Feedback coefficients."""

    @property
    def coefficients() -> NaiveFeedbackStrategy:
        """Coefficients."""

    @property
    def feedback() -> list[FeedbackItem]:
        """Feedback items."""

    @property
    def target() -> NamedVector:
        """Target vector."""

class FieldCondition:
    """Condition on a payload field."""

    def __new__(
        key: JsonPath,
        match: MatchType | None = None,
        range: None | RangeType = None,
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
    def key() -> str:
        """Field key."""

    @property
    def match() -> MatchType | None:
        """Match condition."""

    @property
    def range() -> None | RangeType:
        """Range condition."""

    @property
    def values_count() -> None | ValuesCount:
        """Values count."""

class Filter:
    """Filter conditions for queries."""

    def __new__(
        must: None | list[ConditionType] = None,
        should: None | list[ConditionType] = None,
        must_not: None | list[ConditionType] = None,
        min_should: MinShould | None = None,
    ):
        """Create a Filter.

        Args:
            must: Conditions that must all match.
            should: Conditions where at least one should match.
            must_not: Conditions that must not match.
            min_should: Minimum number of should conditions to match."""

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

class Formula:
    """A scoring formula for custom ranking."""

    def __new__(formula: ExpressionType, defaults: None | dict[str, Any] = None):
        """Create a Formula.

        Args:
            formula: Expression tree.
            defaults: Default variable values."""

class Fusion:
    """Fusion methods for combining multiple prefetch results."""

    class Dbsf:
        """DBSF (Distribution-Based Score Fusion)."""

        def __new__(): ...

    class Rrf:
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

        def __new__(k: int, weights: None | list[float] = None): ...
        @property
        def k() -> int: ...
        @property
        def weights() -> None | list[float]: ...

class GeoBoundingBox:
    """A geographic bounding box."""

    def __new__(top_left: GeoPoint, bottom_right: GeoPoint):
        """Create a GeoBoundingBox.

        Args:
            top_left: Top-left corner.
            bottom_right: Bottom-right corner."""

    @property
    def bottom_right() -> GeoPoint:
        """Bottom-right corner."""

    @property
    def top_left() -> GeoPoint:
        """Top-left corner."""

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

class GeoPoint:
    """A geographic point."""

    def __new__(lon: float, lat: float):
        """Create a GeoPoint.

        Args:
            lon: Longitude (-180 to 180).
            lat: Latitude (-90 to 90)."""

    @property
    def lat() -> float:
        """Latitude."""

    @property
    def lon() -> float:
        """Longitude."""

class GeoPolygon:
    """A geographic polygon."""

    def __new__(
        exterior: list[GeoPoint], interiors: None | list[list[GeoPoint]] = None
    ):
        """Create a GeoPolygon.

        Args:
            exterior: Exterior ring points.
            interiors: Optional interior rings (holes)."""

    @property
    def exterior() -> list[GeoPoint]:
        """Exterior ring."""

    @property
    def interiors() -> None | list[list[GeoPoint]]:
        """Interior rings (holes)."""

class GeoRadius:
    """A geographic circle."""

    def __new__(center: GeoPoint, radius: float):
        """Create a GeoRadius.

        Args:
            center: Center point.
            radius: Radius in meters."""

    @property
    def center() -> GeoPoint:
        """Center point."""

    @property
    def radius() -> float:
        """Radius in meters."""

class HasIdCondition:
    """Check if point ID is in a set."""

    def __new__(point_ids: set[PointId]):
        """Create a HasIdCondition.

        Args:
            point_ids: Set of point IDs."""

    @property
    def point_ids() -> set[PointId]:
        """Point IDs."""

class HasVectorCondition:
    """Check if point has a specific vector."""

    def __new__(vector: str):
        """Create a HasVectorCondition.

        Args:
            vector: Vector name."""

    @property
    def vector() -> str:
        """Vector name."""

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

class IdfParams:
    """Population over which sparse vector IDF statistics are computed - the IDF corpus.

    Only applicable to sparse vectors with the IDF modifier enabled."""

    def __new__(corpus: Filter | None = None):
        """Create IdfParams.

        Args:
            corpus: Filter defining the corpus: IDF statistics are computed over
                the points matching this filter. If None, statistics are
                collection-wide (global)."""

    @property
    def corpus() -> Filter | None:
        """Corpus filter, None for global statistics."""

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

class IsEmptyCondition:
    """Check if a field is empty."""

    def __new__(key: JsonPath):
        """Create an IsEmptyCondition.

        Args:
            key: Payload field path."""

    @property
    def key() -> str:
        """Field key."""

class IsNullCondition:
    """Check if a field is null."""

    def __new__(key: JsonPath):
        """Create an IsNullCondition.

        Args:
            key: Payload field path."""

    @property
    def key() -> str:
        """Field key."""

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

class Language(Enum):
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

class MatchAny:
    """Match any of the values."""

    def __new__(any: list[int] | list[str]):
        """Create a MatchAny.

        Args:
            any: List of values to match any of."""

    @property
    def value() -> list[int] | list[str]:
        """Values."""

class MatchExcept:
    """Match any value except these."""

    def __new__(except_: list[int] | list[str]):
        """Create a MatchExcept.

        Args:
            except_: List of values to exclude."""

    @property
    def value() -> list[int] | list[str]:
        """Excluded values."""

class MatchPhrase:
    """Match exact phrase."""

    def __new__(phrase: str):
        """Create a MatchPhrase.

        Args:
            phrase: Phrase to match."""

    @property
    def phrase() -> str:
        """Phrase."""

class MatchPrefix:
    """Match keyword values starting with the given prefix."""

    def __new__(prefix: str):
        """Create a MatchPrefix.

        Args:
            prefix: Prefix to match."""

    @property
    def prefix() -> str:
        """Prefix."""

class MatchSubstring:
    """Match keyword values containing the given substring."""

    def __new__(substring: str):
        """Create a MatchSubstring.

        Args:
            substring: Substring to match."""

    @property
    def substring() -> str:
        """Substring."""

class MatchText:
    """Full-text match."""

    def __new__(text: str):
        """Create a MatchText.

        Args:
            text: Text to search for."""

    @property
    def text() -> str:
        """Text."""

class MatchTextAny:
    """Match any of the words in text."""

    def __new__(text_any: str):
        """Create a MatchTextAny.

        Args:
            text_any: Space-separated words to match any of."""

    @property
    def text_any() -> str:
        """Text."""

class MatchValue:
    """Match exact value."""

    def __new__(value: bool | int | str):
        """Create a MatchValue.

        Args:
            value: Value to match."""

    @property
    def value() -> bool | int | str:
        """Value."""

class MinShould:
    """Minimum number of should conditions that must match."""

    def __new__(conditions: list[ConditionType], min_count: int):
        """Create a MinShould.

        Args:
            conditions: List of conditions.
            min_count: Minimum number that must match."""

    @property
    def conditions() -> list[ConditionType]:
        """Conditions."""

    @property
    def min_count() -> int:
        """Minimum count."""

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

class Modifier(Enum):
    """Sparse vector modifiers."""

    Idf: Final[Modifier]

class MultiVectorComparator(Enum):
    """Multi-vector comparison methods."""

    MaxSim: Final[MultiVectorComparator]

class MultiVectorConfig:
    """Configuration for multi-vector storage."""

    def __new__(comparator: MultiVectorComparator):
        """Create a MultiVectorConfig.

        Args:
            comparator: Multi-vector comparator."""

    @property
    def comparator() -> MultiVectorComparator:
        """Comparator."""

class NaiveFeedbackStrategy:
    """Coefficients for naive feedback query."""

    def __new__(a: float, b: float, c: float):
        """Create NaiveFeedbackStrategy coefficients.

        Args:
            a: Coefficient a.
            b: Coefficient b.
            c: Coefficient c."""

    @property
    def a() -> float:
        """Coefficient a."""

    @property
    def b() -> float:
        """Coefficient b."""

    @property
    def c() -> float:
        """Coefficient c."""

class NestedCondition:
    """Condition on nested objects."""

    def __new__(key: JsonPath, filter: Filter):
        """Create a NestedCondition.

        Args:
            key: Path to nested array.
            filter: Filter to apply to nested objects."""

    @property
    def filter() -> Filter:
        """Nested filter."""

    @property
    def key() -> str:
        """Nested field key."""

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

    @property
    def direction() -> Direction | None:
        """Sort direction."""

    @property
    def key() -> str:
        """Field key."""

    @property
    def start_from() -> None | StartFromType:
        """Starting value."""

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

class PayloadSchemaType(Enum):
    """Payload field schema types."""

    Bool: Final[PayloadSchemaType]
    Datetime: Final[PayloadSchemaType]
    Float: Final[PayloadSchemaType]
    Geo: Final[PayloadSchemaType]
    Integer: Final[PayloadSchemaType]
    Keyword: Final[PayloadSchemaType]
    Text: Final[PayloadSchemaType]
    Uuid: Final[PayloadSchemaType]

class PayloadSelector(Enum):
    """Select specific payload fields."""

    @staticmethod
    def Exclude(keys: list[str]) -> PayloadSelector:
        """Exclude specified fields."""

    @staticmethod
    def Include(keys: list[str]) -> PayloadSelector:
        """Include only specified fields."""

class PlainIndexConfig:
    """Configuration for plain (brute-force) index."""

    def __new__():
        """Create a PlainIndexConfig."""

class Point:
    """A point with ID, vector(s), and optional payload."""

    def __new__(id: PointId, vector: Vector, payload: None | Payload = None):
        """Create a Point.

        Args:
            id: Point ID (integer or UUID).
            vector: Vector data.
            payload: Optional payload dictionary."""

    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def payload() -> None | Payload:
        """Payload."""

    @property
    def vector() -> Vector:
        """Vector data."""

class PointVectors:
    """Point ID with associated vectors for update operations."""

    def __new__(id: PointId, vector: Vector):
        """Create a PointVectors.

        Args:
            id: Point ID.
            vector: Vector data."""

    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def vector() -> Vector:
        """Vector data."""

class Prefetch:
    """A prefetch stage for multi-stage queries."""

    def __new__(
        limit: int,
        query: None | ScoringQueryType = None,
        prefetches: None | list[Prefetch] = None,
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

class ProductQuantizationConfig:
    """Configuration for product quantization."""

    def __new__(compression: CompressionRatio, always_ram: None | bool = None):
        """Create a ProductQuantizationConfig.

        Args:
            compression: Compression ratio.
            always_ram: Whether to keep in RAM."""

    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def compression() -> CompressionRatio:
        """Compression ratio."""

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

    @property
    def ignore() -> bool:
        """Ignore quantization flag."""

    @property
    def oversampling() -> None | float:
        """Oversampling factor."""

    @property
    def rescore() -> None | bool:
        """Rescore flag."""

class Query(Enum):
    """Query types for vector search."""

    @staticmethod
    def Context(query: ContextQuery, using: None | str = None) -> Query:
        """Create a context query."""

    @staticmethod
    def Discover(query: DiscoverQuery, using: None | str = None) -> Query:
        """Create a discover query."""

    @staticmethod
    def FeedbackNaive(query: FeedbackNaiveQuery, using: None | str = None) -> Query:
        """Create a feedback naive query."""

    @staticmethod
    def Nearest(query: NamedVector, using: None | str = None) -> Query:
        """Create a nearest neighbor query."""

    @staticmethod
    def RecommendBestScore(query: RecommendQuery, using: None | str = None) -> Query:
        """Create a recommend query using best score."""

    @staticmethod
    def RecommendSumScores(query: RecommendQuery, using: None | str = None) -> Query:
        """Create a recommend query using sum of scores."""

class QueryBatchRequest:
    def __new__(queries: list[QueryRequest]):
        """Create a batch of queries, returning results in the same order."""

    def __repr__() -> str: ...
    @property
    def queries() -> list[QueryRequest]: ...

class QueryRequest:
    """Request for query operation."""

    def __new__(
        limit: int,
        offset: None | int = None,
        query: None | ScoringQueryType = None,
        prefetches: None | list[Prefetch] = None,
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

class RecommendQuery:
    """Query for recommendation based on positive and negative examples."""

    def __new__(positives: list[NamedVector], negatives: list[NamedVector]):
        """Create a RecommendQuery.

        Args:
            positives: Positive example vectors.
            negatives: Negative example vectors."""

    @property
    def negatives() -> list[NamedVector]:
        """Negative examples."""

    @property
    def positives() -> list[NamedVector]:
        """Positive examples."""

class Record:
    """A retrieved point record."""

    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def order_value() -> None | (float | int):
        """Order value for order_by queries."""

    @property
    def payload() -> None | Payload:
        """Payload (if requested)."""

    @property
    def vector() -> None | Vector:
        """Vector data (if requested)."""

class Sample(Enum):
    """Sampling methods."""

    Random: Final[Sample]

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

    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def quantile() -> None | float:
        """Quantile."""

    @property
    def type() -> ScalarType:
        """Scalar type."""

class ScalarType(Enum):
    """Scalar quantization types."""

    Int8: Final[ScalarType]

class ScoredPoint:
    """A point with a similarity score."""

    @property
    def id() -> PointId:
        """Point ID."""

    @property
    def order_value() -> None | (float | int):
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
            idf: Population sparse IDF statistics are computed over."""

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

class ShardInfo:
    """Information about a shard."""

    @property
    def indexed_vectors_count() -> int:
        """Number of indexed vectors."""

    @property
    def payload_schema() -> dict[str, PayloadIndexInfo]:
        """Payload schema information."""

    @property
    def points_count() -> int:
        """Number of points."""

    @property
    def segments_count() -> int:
        """Number of segments."""

class SnowballLanguage(Enum):
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

class SnowballParams:
    """Snowball stemming algorithm parameters."""

    def __new__(language: SnowballLanguage):
        """Create SnowballParams.

        Args:
            language: Snowball language."""

    @property
    def language() -> SnowballLanguage:
        """Snowball language."""

class SparseVector:
    """A sparse vector representation."""

    def __new__(indices: list[int], values: list[float]):
        """Create a SparseVector.

        Args:
            indices: Non-zero dimension indices.
            values: Values at the non-zero dimensions."""

    @property
    def indices() -> list[int]:
        """Non-zero dimension indices."""

    @property
    def values() -> list[float]:
        """Values at non-zero dimensions."""

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

class TokenizerType(Enum):
    """Text tokenizer types."""

    Multilingual: Final[TokenizerType]
    Prefix: Final[TokenizerType]
    Whitespace: Final[TokenizerType]
    Word: Final[TokenizerType]

class TurboQuantBitSize(Enum):
    """TurboQuant bit size for compressed codes."""

    Bits1: Final[TurboQuantBitSize]
    Bits1_5: Final[TurboQuantBitSize]
    Bits2: Final[TurboQuantBitSize]
    Bits4: Final[TurboQuantBitSize]

class TurboQuantQuantizationConfig:
    """Configuration for TurboQuant quantization."""

    def __new__(
        always_ram: None | bool = None,
        plus: None | bool = None,
        bits: None | TurboQuantBitSize = None,
    ):
        """Create a TurboQuantQuantizationConfig.

        Args:
            always_ram: Whether to keep in RAM.
            plus: Enable the TurboQuant+ variant.
            bits: Bit size used for compressed codes."""

    @property
    def always_ram() -> None | bool:
        """Always RAM flag."""

    @property
    def bits() -> None | TurboQuantBitSize:
        """Bit size."""

    @property
    def plus() -> None | bool:
        """TurboQuant+ flag."""

class UpdateMode(Enum):
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

class UpdateOperation:
    """Operations for updating shard data."""

    @staticmethod
    def clear_payload(point_ids: list[PointId]) -> UpdateOperation:
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
        field_name: str, schema: PayloadSchemaParams | PayloadSchemaType
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
    def delete_field_index(field_name: str) -> UpdateOperation:
        """Delete an index from a payload field.

        Args:
            field_name: Path to the payload field."""

    @staticmethod
    def delete_payload(point_ids: list[PointId], keys: list[str]) -> UpdateOperation:
        """Delete payload fields from points.

        Args:
            point_ids: Point IDs.
            keys: Payload field keys to delete."""

    @staticmethod
    def delete_payload_by_filter(filter: Filter, keys: list[str]) -> UpdateOperation:
        """Delete payload fields from points matching a filter.

        Args:
            filter: Filter for points.
            keys: Payload field keys to delete."""

    @staticmethod
    def delete_points(point_ids: list[PointId]) -> UpdateOperation:
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
        point_ids: list[PointId], vector_names: list[str]
    ) -> UpdateOperation:
        """Delete specific vectors from points.

        Args:
            point_ids: Point IDs.
            vector_names: Names of vectors to delete."""

    @staticmethod
    def delete_vectors_by_filter(
        filter: Filter, vector_names: list[str]
    ) -> UpdateOperation:
        """Delete vectors from points matching a filter.

        Args:
            filter: Filter for points.
            vector_names: Names of vectors to delete."""

    @staticmethod
    def overwrite_payload(
        point_ids: list[PointId], payload: Payload, key: None | str = None
    ) -> UpdateOperation:
        """Overwrite entire payload on points.

        Args:
            point_ids: Point IDs.
            payload: New payload.
            key: Optional nested key path."""

    @staticmethod
    def overwrite_payload_by_filter(
        filter: Filter, payload: Payload, key: None | str = None
    ) -> UpdateOperation:
        """Overwrite payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: New payload.
            key: Optional nested key path."""

    @staticmethod
    def set_payload(
        point_ids: list[PointId], payload: Payload, key: None | str = None
    ) -> UpdateOperation:
        """Set payload fields on points.

        Args:
            point_ids: Point IDs.
            payload: Payload to set.
            key: Optional nested key path."""

    @staticmethod
    def set_payload_by_filter(
        filter: Filter, payload: Payload, key: None | str = None
    ) -> UpdateOperation:
        """Set payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: Payload to set.
            key: Optional nested key path."""

    @staticmethod
    def update_vectors(
        point_vectors: list[PointVectors], condition: Filter | None = None
    ) -> UpdateOperation:
        """Update vectors of existing points.

        Args:
            point_vectors: Point IDs with new vectors.
            condition: Optional filter condition."""

    @staticmethod
    def upsert_points(
        points: list[Point],
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

class VectorStorageDatatype(Enum):
    """Vector storage data types."""

    Float16: Final[VectorStorageDatatype]
    Float32: Final[VectorStorageDatatype]
    Uint8: Final[VectorStorageDatatype]
