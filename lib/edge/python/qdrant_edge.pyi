"""
Type stubs for qdrant_edge - Qdrant Edge Python bindings.

This module provides type hints for the qdrant_edge package to enable
IDE autocompletion and type checking.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Union
from enum import Enum
from uuid import UUID

# Type aliases
PointId = Union[int, UUID, str]
Vector = Union[List[float], List[List[float]], Dict[str, "NamedVector"]]
NamedVector = Union[List[float], "SparseVector", List[List[float]]]
Payload = Dict[str, Any]
JsonPath = str
WithPayloadType = Union[bool, List[str], "PayloadSelector"]
WithVectorType = Union[bool, List[str]]
ScoringQueryType = Union["Query", "Fusion", "OrderBy", "Formula", "Sample", "Mmr"]
ConditionType = Union[
    "FieldCondition",
    "IsEmptyCondition",
    "IsNullCondition",
    "HasIdCondition",
    "HasVectorCondition",
    "NestedCondition",
    "Filter",
]
MatchType = Union[
    "MatchValue", "MatchText", "MatchTextAny", "MatchPhrase", "MatchAny", "MatchExcept"
]
RangeType = Union["RangeFloat", "RangeDateTime"]
QuantizationConfigType = Union[
    "ScalarQuantizationConfig", "ProductQuantizationConfig", "BinaryQuantizationConfig"
]
IndexType = Union["PlainIndexConfig", "HnswIndexConfig"]
StartFromType = Union[int, float, str]
ExpressionType = "Expression"


# ============================================================================
# Main EdgeShard Class
# ============================================================================


class EdgeShard:
    """
    The main class representing a Qdrant Edge shard.

    A shard is a self-contained unit of storage that can be loaded, queried,
    and updated independently.
    """

    def __init__(
            self,
            path: str,
            config: Optional["EdgeConfig"] = None,
    ) -> None:
        """
        Load or create a Qdrant Edge shard.

        Args:
            path: Path to the shard directory.
            config: Optional configuration for creating a new shard.
        """
        ...

    def flush(self) -> None:
        """Flush all pending changes to disk."""
        ...

    def close(self) -> None:
        """Close the shard and release all resources."""
        ...

    def update(self, operation: "UpdateOperation") -> None:
        """
        Apply an update operation to the shard.

        Args:
            operation: The update operation to apply.
        """
        ...

    def query(self, query: "QueryRequest") -> List["ScoredPoint"]:
        """
        Execute a query against the shard.

        Args:
            query: The query request.

        Returns:
            List of scored points matching the query.
        """
        ...

    def search(self, search: "SearchRequest") -> List["ScoredPoint"]:
        """
        Execute a search against the shard.

        Args:
            search: The search request.

        Returns:
            List of scored points matching the search.
        """
        ...

    def scroll(
            self, scroll: "ScrollRequest"
    ) -> Tuple[List["Record"], Optional[PointId]]:
        """
        Scroll through points in the shard.

        Args:
            scroll: The scroll request.

        Returns:
            Tuple of (points, next_offset).
        """
        ...

    def count(self, count: "CountRequest") -> int:
        """
        Count points in the shard.

        Args:
            count: The count request.

        Returns:
            Number of points matching the filter.
        """
        ...

    def facet(self, facet: "FacetRequest") -> "FacetResponse":
        """
        Get facets for a payload field.

        Args:
            facet: The facet request.

        Returns:
            Facet response with hits and counts.
        """
        ...

    def retrieve(
            self,
            point_ids: List[PointId],
            with_payload: Optional[WithPayloadType] = None,
            with_vector: Optional[WithVectorType] = None,
    ) -> List["Record"]:
        """
        Retrieve specific points by their IDs.

        Args:
            point_ids: List of point IDs to retrieve.
            with_payload: Whether to include payload in results.
            with_vector: Whether to include vectors in results.

        Returns:
            List of records.
        """
        ...

    def info(self) -> "ShardInfo":
        """
        Get information about the shard.

        Returns:
            Shard information.
        """
        ...

    @staticmethod
    def unpack_snapshot(snapshot_path: str, target_path: str) -> None:
        """
        Unpack a snapshot to a target directory.

        Args:
            snapshot_path: Path to the snapshot file.
            target_path: Path to extract the snapshot to.
        """
        ...

    def snapshot_manifest(self) -> Any:
        """
        Get the snapshot manifest.

        Returns:
            Snapshot manifest as a JSON-like value.
        """
        ...

    def update_from_snapshot(
            self,
            snapshot_path: str,
            tmp_dir: Optional[str] = None,
    ) -> None:
        """
        Update the shard from a snapshot.

        Args:
            snapshot_path: Path to the snapshot file.
            tmp_dir: Optional temporary directory for extraction.
        """
        ...


# ============================================================================
# Configuration Classes
# ============================================================================


class EdgeConfig:
    """Configuration for creating a new Qdrant Edge shard."""

    def __init__(
            self,
            vector_data: Union["VectorDataConfig", Dict[str, "VectorDataConfig"]],
            sparse_vector_data: Optional[Dict[str, "SparseVectorDataConfig"]] = None,
    ) -> None:
        """
        Create an EdgeConfig.

        Args:
            vector_data: Dense vector configuration. Can be a single config for
                        the default vector or a dict of named vector configs.
            sparse_vector_data: Optional sparse vector configurations.
        """
        ...

    @property
    def vector_data(self) -> Dict[str, "VectorDataConfig"]:
        """Dense vector configurations."""
        ...

    @property
    def sparse_vector_data(self) -> Dict[str, "SparseVectorDataConfig"]:
        """Sparse vector configurations."""
        ...

    @property
    def payload_storage_type(self) -> "PayloadStorageType":
        """Payload storage type."""
        ...


class VectorDataConfig:
    """Configuration for dense vector storage."""

    def __init__(
            self,
            size: int,
            distance: "Distance",
            quantization_config: Optional[QuantizationConfigType] = None,
            multivector_config: Optional["MultiVectorConfig"] = None,
            datatype: Optional["VectorStorageDatatype"] = None,
    ) -> None:
        """
        Create a VectorDataConfig.

        Args:
            size: Dimension of vectors.
            distance: Distance metric.
            quantization_config: Optional quantization configuration.
            multivector_config: Optional multi-vector configuration.
            datatype: Optional storage datatype.
        """
        ...

    @property
    def size(self) -> int:
        """Vector dimension."""
        ...

    @property
    def distance(self) -> "Distance":
        """Distance metric."""
        ...

    @property
    def storage_type(self) -> "VectorStorageType":
        """Storage type."""
        ...

    @property
    def index(self) -> IndexType:
        """Index configuration."""
        ...

    @property
    def quantization_config(self) -> Optional[QuantizationConfigType]:
        """Quantization configuration."""
        ...

    @property
    def multivector_config(self) -> Optional["MultiVectorConfig"]:
        """Multi-vector configuration."""
        ...

    @property
    def datatype(self) -> Optional["VectorStorageDatatype"]:
        """Storage datatype."""
        ...


class SparseVectorDataConfig:
    """Configuration for sparse vector storage."""

    def __init__(
            self,
            index: "SparseIndexConfig",
            modifier: Optional["Modifier"] = None,
    ) -> None:
        """
        Create a SparseVectorDataConfig.

        Args:
            index: Sparse index configuration.
            modifier: Optional modifier (e.g., IDF).
        """
        ...

    @property
    def index(self) -> "SparseIndexConfig":
        """Index configuration."""
        ...

    @property
    def storage_type(self) -> "SparseVectorStorageType":
        """Storage type."""
        ...

    @property
    def modifier(self) -> Optional["Modifier"]:
        """Modifier."""
        ...


class SparseIndexConfig:
    """Configuration for sparse vector index."""

    def __init__(
            self,
            full_scan_threshold: Optional[int] = None,
            datatype: Optional["VectorStorageDatatype"] = None,
    ) -> None:
        """
        Create a SparseIndexConfig.

        Args:
            full_scan_threshold: Threshold for full scan vs index search.
            datatype: Storage datatype.
        """
        ...

    @property
    def full_scan_threshold(self) -> Optional[int]:
        """Full scan threshold."""
        ...

    @property
    def index_type(self) -> "SparseIndexType":
        """Index type."""
        ...

    @property
    def datatype(self) -> Optional["VectorStorageDatatype"]:
        """Storage datatype."""
        ...


class PlainIndexConfig:
    """Configuration for plain (brute-force) index."""

    def __init__(self) -> None:
        """Create a PlainIndexConfig."""
        ...


class HnswIndexConfig:
    """Configuration for HNSW index."""

    def __init__(
            self,
            m: int,
            ef_construct: int,
            full_scan_threshold: int,
            on_disk: Optional[bool] = None,
            payload_m: Optional[int] = None,
            inline_storage: Optional[bool] = None,
    ) -> None:
        """
        Create an HnswIndexConfig.

        Args:
            m: Number of edges per node.
            ef_construct: Number of candidates during index construction.
            full_scan_threshold: Threshold for full scan.
            on_disk: Whether to store on disk.
            payload_m: Payload index m value.
            inline_storage: Whether to use inline storage.
        """
        ...

    @property
    def m(self) -> int:
        """Number of edges per node."""
        ...

    @property
    def ef_construct(self) -> int:
        """ef_construct value."""
        ...

    @property
    def full_scan_threshold(self) -> int:
        """Full scan threshold."""
        ...

    @property
    def on_disk(self) -> Optional[bool]:
        """On-disk flag."""
        ...

    @property
    def payload_m(self) -> Optional[int]:
        """Payload m value."""
        ...

    @property
    def inline_storage(self) -> Optional[bool]:
        """Inline storage flag."""
        ...


class MultiVectorConfig:
    """Configuration for multi-vector storage."""

    def __init__(self, comparator: "MultiVectorComparator") -> None:
        """
        Create a MultiVectorConfig.

        Args:
            comparator: Multi-vector comparator.
        """
        ...

    @property
    def comparator(self) -> "MultiVectorComparator":
        """Comparator."""
        ...


# ============================================================================
# Quantization Configuration
# ============================================================================


class ScalarQuantizationConfig:
    """Configuration for scalar quantization."""

    def __init__(
            self,
            type: "ScalarType",
            quantile: Optional[float] = None,
            always_ram: Optional[bool] = None,
    ) -> None:
        """
        Create a ScalarQuantizationConfig.

        Args:
            type: Scalar type (e.g., Int8).
            quantile: Quantile for normalization.
            always_ram: Whether to keep in RAM.
        """
        ...

    @property
    def type(self) -> "ScalarType":
        """Scalar type."""
        ...

    @property
    def quantile(self) -> Optional[float]:
        """Quantile."""
        ...

    @property
    def always_ram(self) -> Optional[bool]:
        """Always RAM flag."""
        ...


class ProductQuantizationConfig:
    """Configuration for product quantization."""

    def __init__(
            self,
            compression: "CompressionRatio",
            always_ram: Optional[bool] = None,
    ) -> None:
        """
        Create a ProductQuantizationConfig.

        Args:
            compression: Compression ratio.
            always_ram: Whether to keep in RAM.
        """
        ...

    @property
    def compression(self) -> "CompressionRatio":
        """Compression ratio."""
        ...

    @property
    def always_ram(self) -> Optional[bool]:
        """Always RAM flag."""
        ...


class BinaryQuantizationConfig:
    """Configuration for binary quantization."""

    def __init__(
            self,
            always_ram: Optional[bool] = None,
            encoding: Optional["BinaryQuantizationEncoding"] = None,
            query_encoding: Optional["BinaryQuantizationQueryEncoding"] = None,
    ) -> None:
        """
        Create a BinaryQuantizationConfig.

        Args:
            always_ram: Whether to keep in RAM.
            encoding: Binary encoding type.
            query_encoding: Query encoding type.
        """
        ...

    @property
    def always_ram(self) -> Optional[bool]:
        """Always RAM flag."""
        ...

    @property
    def encoding(self) -> Optional["BinaryQuantizationEncoding"]:
        """Encoding."""
        ...

    @property
    def query_encoding(self) -> Optional["BinaryQuantizationQueryEncoding"]:
        """Query encoding."""
        ...


# ============================================================================
# Enums
# ============================================================================


class Distance(Enum):
    """Distance metrics for vector comparison."""

    Cosine = ...
    Euclid = ...
    Dot = ...
    Manhattan = ...


class VectorStorageType(Enum):
    """Vector storage types."""

    Memory = ...
    Mmap = ...
    ChunkedMmap = ...
    InRamChunkedMmap = ...
    InRamMmap = ...


class VectorStorageDatatype(Enum):
    """Vector storage data types."""

    Float32 = ...
    Float16 = ...
    Uint8 = ...


class PayloadStorageType(Enum):
    """Payload storage types."""

    Mmap = ...
    InRamMmap = ...


class MultiVectorComparator(Enum):
    """Multi-vector comparison methods."""

    MaxSim = ...


class ScalarType(Enum):
    """Scalar quantization types."""

    Int8 = ...


class CompressionRatio(Enum):
    """Product quantization compression ratios."""

    X4 = ...
    X8 = ...
    X16 = ...
    X32 = ...
    X64 = ...


class BinaryQuantizationEncoding(Enum):
    """Binary quantization encoding types."""

    OneBit = ...
    TwoBits = ...
    OneAndHalfBits = ...


class BinaryQuantizationQueryEncoding(Enum):
    """Binary quantization query encoding types."""

    Default = ...
    Binary = ...
    Scalar4Bits = ...
    Scalar8Bits = ...


class SparseIndexType(Enum):
    """Sparse index types."""

    MutableRam = ...
    ImmutableRam = ...
    Mmap = ...


class SparseVectorStorageType(Enum):
    """Sparse vector storage types."""

    Mmap = ...


class Modifier(Enum):
    """Sparse vector modifiers."""

    # Note: Python reserved word 'None' cannot be used as enum member
    Idf = ...


class UpdateMode(Enum):
    """Defines the mode of the upsert operation."""

    Upsert = ...
    """Default mode - insert new points, update existing points."""
    InsertOnly = ...
    """Only insert new points, do not update existing points."""
    UpdateOnly = ...
    """Only update existing points, do not insert new points."""


class Direction(Enum):
    """Sort direction."""

    Asc = ...
    Desc = ...


class Sample(Enum):
    """Sampling methods."""

    Random = ...


class DecayKind(Enum):
    """Decay function kinds for scoring formulas."""

    Lin = ...
    Gauss = ...
    Exp = ...


class PayloadSchemaType(Enum):
    """Payload field schema types."""

    Keyword = ...
    Integer = ...
    Float = ...
    Geo = ...
    Text = ...
    Bool = ...
    Datetime = ...
    Uuid = ...


# ============================================================================
# Data Types
# ============================================================================


class Point:
    """A point with ID, vector(s), and optional payload."""

    def __init__(
            self,
            id: PointId,
            vector: Vector,
            payload: Optional[Payload] = None,
    ) -> None:
        """
        Create a Point.

        Args:
            id: Point ID (integer or UUID).
            vector: Vector data.
            payload: Optional payload dictionary.
        """
        ...

    @property
    def id(self) -> PointId:
        """Point ID."""
        ...

    @property
    def vector(self) -> Vector:
        """Vector data."""
        ...

    @property
    def payload(self) -> Optional[Payload]:
        """Payload."""
        ...


class PointVectors:
    """Point ID with associated vectors for update operations."""

    def __init__(self, id: PointId, vector: Vector) -> None:
        """
        Create a PointVectors.

        Args:
            id: Point ID.
            vector: Vector data.
        """
        ...

    @property
    def id(self) -> PointId:
        """Point ID."""
        ...

    @property
    def vector(self) -> Vector:
        """Vector data."""
        ...


class SparseVector:
    """A sparse vector representation."""

    def __init__(self, indices: List[int], values: List[float]) -> None:
        """
        Create a SparseVector.

        Args:
            indices: Non-zero dimension indices.
            values: Values at the non-zero dimensions.
        """
        ...

    @property
    def indices(self) -> List[int]:
        """Non-zero dimension indices."""
        ...

    @property
    def values(self) -> List[float]:
        """Values at non-zero dimensions."""
        ...


class ScoredPoint:
    """A point with a similarity score."""

    @property
    def id(self) -> PointId:
        """Point ID."""
        ...

    @property
    def version(self) -> int:
        """Point version."""
        ...

    @property
    def score(self) -> float:
        """Similarity score."""
        ...

    @property
    def vector(self) -> Optional[Vector]:
        """Vector data (if requested)."""
        ...

    @property
    def payload(self) -> Optional[Payload]:
        """Payload (if requested)."""
        ...

    @property
    def order_value(self) -> Optional[Union[int, float]]:
        """Order value for order_by queries."""
        ...


class Record:
    """A retrieved point record."""

    @property
    def id(self) -> PointId:
        """Point ID."""
        ...

    @property
    def vector(self) -> Optional[Vector]:
        """Vector data (if requested)."""
        ...

    @property
    def payload(self) -> Optional[Payload]:
        """Payload (if requested)."""
        ...

    @property
    def order_value(self) -> Optional[Union[int, float]]:
        """Order value for order_by queries."""
        ...


class ShardInfo:
    """Information about a shard."""

    @property
    def segments_count(self) -> int:
        """Number of segments."""
        ...

    @property
    def points_count(self) -> int:
        """Number of points."""
        ...

    @property
    def indexed_vectors_count(self) -> int:
        """Number of indexed vectors."""
        ...

    @property
    def payload_schema(self) -> Dict[str, "PayloadIndexInfo"]:
        """Payload schema information."""
        ...


class PayloadIndexInfo:
    """Information about a payload index."""

    @property
    def data_type(self) -> PayloadSchemaType:
        """Data type."""
        ...

    @property
    def params(self) -> Optional[Any]:
        """Index parameters."""
        ...

    @property
    def points(self) -> int:
        """Number of points with this field."""
        ...


# ============================================================================
# Request Classes
# ============================================================================


class QueryRequest:
    """Request for query operation."""

    def __init__(
            self,
            limit: int,
            offset: Optional[int] = None,
            query: Optional[ScoringQueryType] = None,
            prefetches: Optional[List["Prefetch"]] = None,
            with_vector: Optional[WithVectorType] = None,
            with_payload: Optional[WithPayloadType] = None,
            filter: Optional["Filter"] = None,
            score_threshold: Optional[float] = None,
            params: Optional["SearchParams"] = None,
    ) -> None:
        """
        Create a QueryRequest.

        Args:
            limit: Maximum number of results.
            offset: Number of results to skip.
            query: Scoring query (vector, fusion, order_by, etc.).
            prefetches: Prefetch stages for multi-stage queries.
            with_vector: Whether to include vectors.
            with_payload: Whether to include payload.
            filter: Filter conditions.
            score_threshold: Minimum score threshold.
            params: Search parameters.
        """
        ...

    @property
    def prefetches(self) -> List["Prefetch"]:
        """Prefetch stages."""
        ...

    @property
    def query(self) -> Optional[ScoringQueryType]:
        """Scoring query."""
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...

    @property
    def score_threshold(self) -> Optional[float]:
        """Score threshold."""
        ...

    @property
    def limit(self) -> int:
        """Result limit."""
        ...

    @property
    def offset(self) -> int:
        """Result offset."""
        ...

    @property
    def params(self) -> Optional["SearchParams"]:
        """Search parameters."""
        ...

    @property
    def with_vector(self) -> WithVectorType:
        """With vector flag."""
        ...

    @property
    def with_payload(self) -> WithPayloadType:
        """With payload flag."""
        ...


class Prefetch:
    """A prefetch stage for multi-stage queries."""

    def __init__(
            self,
            limit: int,
            query: Optional[ScoringQueryType] = None,
            prefetches: Optional[List["Prefetch"]] = None,
            params: Optional["SearchParams"] = None,
            filter: Optional["Filter"] = None,
            score_threshold: Optional[float] = None,
    ) -> None:
        """
        Create a Prefetch stage.

        Args:
            limit: Maximum number of results for this stage.
            query: Scoring query.
            prefetches: Nested prefetch stages.
            params: Search parameters.
            filter: Filter conditions.
            score_threshold: Minimum score threshold.
        """
        ...

    @property
    def prefetches(self) -> List["Prefetch"]:
        """Nested prefetch stages."""
        ...

    @property
    def query(self) -> Optional[ScoringQueryType]:
        """Scoring query."""
        ...

    @property
    def limit(self) -> int:
        """Result limit."""
        ...

    @property
    def params(self) -> Optional["SearchParams"]:
        """Search parameters."""
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...

    @property
    def score_threshold(self) -> Optional[float]:
        """Score threshold."""
        ...


class SearchRequest:
    """Request for search operation."""

    def __init__(
            self,
            query: "Query",
            limit: int,
            offset: Optional[int] = None,
            filter: Optional["Filter"] = None,
            params: Optional["SearchParams"] = None,
            with_vector: Optional[WithVectorType] = None,
            with_payload: Optional[WithPayloadType] = None,
            score_threshold: Optional[float] = None,
    ) -> None:
        """
        Create a SearchRequest.

        Args:
            query: Query (vector-based).
            limit: Maximum number of results.
            offset: Number of results to skip.
            filter: Filter conditions.
            params: Search parameters.
            with_vector: Whether to include vectors.
            with_payload: Whether to include payload.
            score_threshold: Minimum score threshold.
        """
        ...

    @property
    def query(self) -> "Query":
        """Query."""
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...

    @property
    def params(self) -> Optional["SearchParams"]:
        """Search parameters."""
        ...

    @property
    def limit(self) -> int:
        """Result limit."""
        ...

    @property
    def offset(self) -> int:
        """Result offset."""
        ...

    @property
    def with_vector(self) -> Optional[WithVectorType]:
        """With vector flag."""
        ...

    @property
    def with_payload(self) -> Optional[WithPayloadType]:
        """With payload flag."""
        ...

    @property
    def score_threshold(self) -> Optional[float]:
        """Score threshold."""
        ...


class ScrollRequest:
    """Request for scroll operation."""

    def __init__(
            self,
            offset: Optional[PointId] = None,
            limit: Optional[int] = None,
            filter: Optional["Filter"] = None,
            with_payload: Optional[WithPayloadType] = None,
            with_vector: Optional[WithVectorType] = None,
            order_by: Optional["OrderBy"] = None,
    ) -> None:
        """
        Create a ScrollRequest.

        Args:
            offset: Starting point ID.
            limit: Maximum number of results.
            filter: Filter conditions.
            with_payload: Whether to include payload.
            with_vector: Whether to include vectors.
            order_by: Order by configuration.
        """
        ...

    @property
    def offset(self) -> Optional[PointId]:
        """Offset point ID."""
        ...

    @property
    def limit(self) -> Optional[int]:
        """Result limit."""
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...

    @property
    def with_payload(self) -> Optional[WithPayloadType]:
        """With payload flag."""
        ...

    @property
    def with_vector(self) -> WithVectorType:
        """With vector flag."""
        ...

    @property
    def order_by(self) -> Optional["OrderBy"]:
        """Order by configuration."""
        ...


class CountRequest:
    """Request for count operation."""

    def __init__(
            self,
            exact: bool = True,
            filter: Optional["Filter"] = None,
    ) -> None:
        """
        Create a CountRequest.

        Args:
            exact: Whether to count exactly or estimate.
            filter: Filter conditions.
        """
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...

    @property
    def exact(self) -> bool:
        """Exact count flag."""
        ...


class FacetRequest:
    """Request for facet operation."""

    def __init__(
            self,
            key: JsonPath,
            limit: int = 10,
            exact: bool = False,
            filter: Optional["Filter"] = None,
    ) -> None:
        """
        Create a FacetRequest.

        Args:
            key: Payload field key to facet on.
            limit: Maximum number of facet hits to return.
            exact: Whether to count exactly or estimate.
            filter: Filter conditions.
        """
        ...

    @property
    def key(self) -> str:
        """Facet key."""
        ...

    @property
    def limit(self) -> int:
        """Result limit."""
        ...

    @property
    def exact(self) -> bool:
        """Exact count flag."""
        ...

    @property
    def filter(self) -> Optional["Filter"]:
        """Filter."""
        ...


class FacetHit:
    """A facet hit with value and count."""

    @property
    def value(self) -> Union[str, int, bool]:
        """Facet value."""
        ...

    @property
    def count(self) -> int:
        """Count of points with this value."""
        ...


class FacetResponse:
    """Response for facet operation."""

    @property
    def hits(self) -> List["FacetHit"]:
        """Facet hits."""
        ...

    def __len__(self) -> int:
        """Number of hits."""
        ...

    def __iter__(self) -> Any:
        """Iterate over hits."""
        ...


class SearchParams:
    """Parameters for search operations."""

    def __init__(
            self,
            hnsw_ef: Optional[int] = None,
            exact: bool = False,
            quantization: Optional["QuantizationSearchParams"] = None,
            indexed_only: bool = False,
            acorn: Optional["AcornSearchParams"] = None,
    ) -> None:
        """
        Create SearchParams.

        Args:
            hnsw_ef: ef parameter for HNSW search.
            exact: Whether to use exact search.
            quantization: Quantization search parameters.
            indexed_only: Whether to search only indexed vectors.
            acorn: Acorn search parameters.
        """
        ...

    @property
    def hnsw_ef(self) -> Optional[int]:
        """HNSW ef parameter."""
        ...

    @property
    def exact(self) -> bool:
        """Exact search flag."""
        ...

    @property
    def quantization(self) -> Optional["QuantizationSearchParams"]:
        """Quantization parameters."""
        ...

    @property
    def indexed_only(self) -> bool:
        """Indexed only flag."""
        ...

    @property
    def acorn(self) -> Optional["AcornSearchParams"]:
        """Acorn parameters."""
        ...


class QuantizationSearchParams:
    """Parameters for quantization during search."""

    def __init__(
            self,
            ignore: bool = False,
            rescore: Optional[bool] = None,
            oversampling: Optional[float] = None,
    ) -> None:
        """
        Create QuantizationSearchParams.

        Args:
            ignore: Whether to ignore quantization.
            rescore: Whether to rescore with original vectors.
            oversampling: Oversampling factor.
        """
        ...

    @property
    def ignore(self) -> bool:
        """Ignore quantization flag."""
        ...

    @property
    def rescore(self) -> Optional[bool]:
        """Rescore flag."""
        ...

    @property
    def oversampling(self) -> Optional[float]:
        """Oversampling factor."""
        ...


class AcornSearchParams:
    """Parameters for Acorn filtered search."""

    def __init__(
            self,
            enable: bool = False,
            max_selectivity: Optional[float] = None,
    ) -> None:
        """
        Create AcornSearchParams.

        Args:
            enable: Whether to enable Acorn.
            max_selectivity: Maximum filter selectivity for Acorn.
        """
        ...

    @property
    def enable(self) -> bool:
        """Enable flag."""
        ...

    @property
    def max_selectivity(self) -> Optional[float]:
        """Maximum selectivity."""
        ...


# ============================================================================
# Query Types
# ============================================================================


class Query(Enum):
    """Query types for vector search."""

    @staticmethod
    def Nearest(
            query: NamedVector, using: Optional[str] = None
    ) -> "Query":
        """Create a nearest neighbor query."""
        ...

    @staticmethod
    def RecommendBestScore(
            query: "RecommendQuery", using: Optional[str] = None
    ) -> "Query":
        """Create a recommend query using best score."""
        ...

    @staticmethod
    def RecommendSumScores(
            query: "RecommendQuery", using: Optional[str] = None
    ) -> "Query":
        """Create a recommend query using sum of scores."""
        ...

    @staticmethod
    def Discover(
            query: "DiscoverQuery", using: Optional[str] = None
    ) -> "Query":
        """Create a discover query."""
        ...

    @staticmethod
    def Context(
            query: "ContextQuery", using: Optional[str] = None
    ) -> "Query":
        """Create a context query."""
        ...

    @staticmethod
    def FeedbackNaive(
            query: "FeedbackNaiveQuery", using: Optional[str] = None
    ) -> "Query":
        """Create a feedback naive query."""
        ...


class Fusion:
    """Fusion methods for combining multiple prefetch results."""

    class Rrf:
        """
        RRF (Reciprocal Rank Fusion) with given parameters.
        
        Args:
            k: The RRF k parameter.
            weights: Optional weights for each prefetch source.
                     Higher weight gives more influence on the final ranking.
                     If not specified, all prefetches are weighted equally.
        
        Examples:
            # Basic RRF with k=2
            Fusion.Rrf(k=2)
            
            # Weighted RRF - first prefetch has 3x weight
            Fusion.Rrf(k=2, weights=[3.0, 1.0])
        """
        def __init__(self, k: int, weights: Optional[List[float]] = None) -> None: ...
        
        @property
        def k(self) -> int: ...
        
        @property
        def weights(self) -> Optional[List[float]]: ...

    class Dbsf:
        """DBSF (Distribution-Based Score Fusion)."""
        def __init__(self) -> None: ...


class OrderBy:
    """Order results by a payload field."""

    def __init__(
            self,
            key: JsonPath,
            direction: Optional[Direction] = None,
            start_from: Optional[StartFromType] = None,
    ) -> None:
        """
        Create an OrderBy.

        Args:
            key: Payload field path.
            direction: Sort direction.
            start_from: Starting value.
        """
        ...

    @property
    def key(self) -> str:
        """Field key."""
        ...

    @property
    def direction(self) -> Optional[Direction]:
        """Sort direction."""
        ...

    @property
    def start_from(self) -> Optional[StartFromType]:
        """Starting value."""
        ...


class Mmr:
    """Maximal Marginal Relevance for result diversification."""

    def __init__(
            self,
            vector: NamedVector,
            lambda_: float,
            candidates_limit: int,
            using: Optional[str] = None,
    ) -> None:
        """
        Create an MMR query.

        Args:
            vector: Query vector.
            lambda_: Balance between relevance and diversity (0-1).
            candidates_limit: Number of candidates to consider.
            using: Named vector to use.
        """
        ...

    @property
    def vector(self) -> NamedVector:
        """Query vector."""
        ...

    @property
    def lambda_(self) -> float:
        """Balance between relevance and diversity."""
        ...

    @property
    def using(self) -> str:
        """Named vector."""
        ...

    # Note: 'lambda' is Python reserved word, using 'lambda_' in __init__
    # but the property may be named differently
    @property
    def candidates_limit(self) -> int:
        """Candidates limit."""
        ...


class RecommendQuery:
    """Query for recommendation based on positive and negative examples."""

    def __init__(
            self,
            positives: List[NamedVector],
            negatives: List[NamedVector],
    ) -> None:
        """
        Create a RecommendQuery.

        Args:
            positives: Positive example vectors.
            negatives: Negative example vectors.
        """
        ...

    @property
    def positives(self) -> List[NamedVector]:
        """Positive examples."""
        ...

    @property
    def negatives(self) -> List[NamedVector]:
        """Negative examples."""
        ...


class DiscoverQuery:
    """Query for discovery using a target and context pairs."""

    def __init__(
            self,
            target: NamedVector,
            pairs: List["ContextPair"],
    ) -> None:
        """
        Create a DiscoverQuery.

        Args:
            target: Target vector.
            pairs: Context pairs.
        """
        ...

    @property
    def target(self) -> NamedVector:
        """Target vector."""
        ...

    @property
    def pairs(self) -> List["ContextPair"]:
        """Context pairs."""
        ...


class ContextQuery:
    """Query based on context pairs only."""

    def __init__(self, pairs: List["ContextPair"]) -> None:
        """
        Create a ContextQuery.

        Args:
            pairs: Context pairs.
        """
        ...

    @property
    def pairs(self) -> List["ContextPair"]:
        """Context pairs."""
        ...


class ContextPair:
    """A positive/negative pair for context-based queries."""

    def __init__(
            self,
            positive: NamedVector,
            negative: NamedVector,
    ) -> None:
        """
        Create a ContextPair.

        Args:
            positive: Positive example.
            negative: Negative example.
        """
        ...

    @property
    def positive(self) -> NamedVector:
        """Positive example."""
        ...

    @property
    def negative(self) -> NamedVector:
        """Negative example."""
        ...


class FeedbackNaiveQuery:
    """Query using naive feedback approach."""

    def __init__(
            self,
            target: NamedVector,
            feedback: List["FeedbackItem"],
            strategy: "NaiveFeedbackStrategy",
    ) -> None:
        """
        Create a FeedbackNaiveQuery.

        Args:
            target: Target vector.
            feedback: Feedback items with scores.
            strategy: Feedback coefficients.
        """
        ...

    @property
    def target(self) -> NamedVector:
        """Target vector."""
        ...

    @property
    def feedback(self) -> List["FeedbackItem"]:
        """Feedback items."""
        ...

    @property
    def coefficients(self) -> "NaiveFeedbackStrategy":
        """Coefficients."""
        ...


class FeedbackItem:
    """A feedback item with vector and score."""

    def __init__(self, vector: NamedVector, score: float) -> None:
        """
        Create a FeedbackItem.

        Args:
            vector: Feedback vector.
            score: Feedback score.
        """
        ...

    @property
    def vector(self) -> NamedVector:
        """Feedback vector."""
        ...

    @property
    def score(self) -> float:
        """Feedback score."""
        ...


class NaiveFeedbackStrategy:
    """Coefficients for naive feedback query."""

    def __init__(self, a: float, b: float, c: float) -> None:
        """
        Create NaiveFeedbackStrategy coefficients.

        Args:
            a: Coefficient a.
            b: Coefficient b.
            c: Coefficient c.
        """
        ...

    @property
    def a(self) -> float:
        """Coefficient a."""
        ...

    @property
    def b(self) -> float:
        """Coefficient b."""
        ...

    @property
    def c(self) -> float:
        """Coefficient c."""
        ...


# ============================================================================
# Formula Classes
# ============================================================================


class Formula:
    """A scoring formula for custom ranking."""

    def __init__(
            self,
            formula: ExpressionType,
            defaults: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create a Formula.

        Args:
            formula: Expression tree.
            defaults: Default variable values.
        """
        ...


class Expression(Enum):
    """Expression types for formulas."""

    @staticmethod
    def Constant(val: float) -> "Expression":
        """Create a constant expression."""
        ...

    @staticmethod
    def Variable(var: str) -> "Expression":
        """Create a variable expression."""
        ...

    @staticmethod
    def Condition(cond: ConditionType) -> "Expression":
        """Create a condition expression (returns 1 if true, 0 if false)."""
        ...

    @staticmethod
    def GeoDistance(origin: "GeoPoint", to: JsonPath) -> "Expression":
        """Create a geo distance expression."""
        ...

    @staticmethod
    def Datetime(date_time: str) -> "Expression":
        """Create a datetime constant expression."""
        ...

    @staticmethod
    def DatetimeKey(path: JsonPath) -> "Expression":
        """Create a datetime field expression."""
        ...

    @staticmethod
    def Mult(exprs: List["Expression"]) -> "Expression":
        """Create a multiplication expression."""
        ...

    @staticmethod
    def Sum(exprs: List["Expression"]) -> "Expression":
        """Create a sum expression."""
        ...

    @staticmethod
    def Neg(expr: "Expression") -> "Expression":
        """Create a negation expression."""
        ...

    @staticmethod
    def Div(
            left: "Expression",
            right: "Expression",
            by_zero_default: Optional[float] = None,
    ) -> "Expression":
        """Create a division expression."""
        ...

    @staticmethod
    def Sqrt(expr: "Expression") -> "Expression":
        """Create a square root expression."""
        ...

    @staticmethod
    def Pow(base: "Expression", exponent: "Expression") -> "Expression":
        """Create a power expression."""
        ...

    @staticmethod
    def Exp(expr: "Expression") -> "Expression":
        """Create an exponential expression."""
        ...

    @staticmethod
    def Log10(expr: "Expression") -> "Expression":
        """Create a log10 expression."""
        ...

    @staticmethod
    def Ln(expr: "Expression") -> "Expression":
        """Create a natural log expression."""
        ...

    @staticmethod
    def Abs(expr: "Expression") -> "Expression":
        """Create an absolute value expression."""
        ...

    @staticmethod
    def Decay(
            kind: DecayKind,
            x: "Expression",
            target: Optional["Expression"] = None,
            midpoint: Optional[float] = None,
            scale: Optional[float] = None,
    ) -> "Expression":
        """Create a decay expression."""
        ...


# ============================================================================
# Filter Classes
# ============================================================================


class Filter:
    """Filter conditions for queries."""

    def __init__(
            self,
            must: Optional[List[ConditionType]] = None,
            should: Optional[List[ConditionType]] = None,
            must_not: Optional[List[ConditionType]] = None,
            min_should: Optional["MinShould"] = None,
    ) -> None:
        """
        Create a Filter.

        Args:
            must: Conditions that must all match.
            should: Conditions where at least one should match.
            must_not: Conditions that must not match.
            min_should: Minimum number of should conditions to match.
        """
        ...

    @property
    def must(self) -> Optional[List[ConditionType]]:
        """Must conditions."""
        ...

    @property
    def should(self) -> Optional[List[ConditionType]]:
        """Should conditions."""
        ...

    @property
    def must_not(self) -> Optional[List[ConditionType]]:
        """Must not conditions."""
        ...

    @property
    def min_should(self) -> Optional["MinShould"]:
        """Minimum should configuration."""
        ...


class MinShould:
    """Minimum number of should conditions that must match."""

    def __init__(
            self,
            conditions: List[ConditionType],
            min_count: int,
    ) -> None:
        """
        Create a MinShould.

        Args:
            conditions: List of conditions.
            min_count: Minimum number that must match.
        """
        ...

    @property
    def conditions(self) -> List[ConditionType]:
        """Conditions."""
        ...

    @property
    def min_count(self) -> int:
        """Minimum count."""
        ...


class FieldCondition:
    """Condition on a payload field."""

    def __init__(
            self,
            key: JsonPath,
            match: Optional[MatchType] = None,
            range: Optional[RangeType] = None,
            geo_bounding_box: Optional["GeoBoundingBox"] = None,
            geo_radius: Optional["GeoRadius"] = None,
            geo_polygon: Optional["GeoPolygon"] = None,
            values_count: Optional["ValuesCount"] = None,
            is_empty: Optional[bool] = None,
            is_null: Optional[bool] = None,
    ) -> None:
        """
        Create a FieldCondition.

        Args:
            key: Payload field path.
            match: Match condition.
            range: Range condition.
            geo_bounding_box: Geo bounding box condition.
            geo_radius: Geo radius condition.
            geo_polygon: Geo polygon condition.
            values_count: Values count condition.
            is_empty: Check if empty.
            is_null: Check if null.
        """
        ...

    @property
    def key(self) -> str:
        """Field key."""
        ...

    @property
    def match(self) -> Optional[MatchType]:
        """Match condition."""
        ...

    @property
    def range(self) -> Optional[RangeType]:
        """Range condition."""
        ...

    @property
    def geo_bounding_box(self) -> Optional["GeoBoundingBox"]:
        """Geo bounding box."""
        ...

    @property
    def geo_radius(self) -> Optional["GeoRadius"]:
        """Geo radius."""
        ...

    @property
    def geo_polygon(self) -> Optional["GeoPolygon"]:
        """Geo polygon."""
        ...

    @property
    def values_count(self) -> Optional["ValuesCount"]:
        """Values count."""
        ...

    @property
    def is_empty(self) -> Optional[bool]:
        """Is empty flag."""
        ...

    @property
    def is_null(self) -> Optional[bool]:
        """Is null flag."""
        ...


class IsEmptyCondition:
    """Check if a field is empty."""

    def __init__(self, key: JsonPath) -> None:
        """
        Create an IsEmptyCondition.

        Args:
            key: Payload field path.
        """
        ...

    @property
    def key(self) -> str:
        """Field key."""
        ...


class IsNullCondition:
    """Check if a field is null."""

    def __init__(self, key: JsonPath) -> None:
        """
        Create an IsNullCondition.

        Args:
            key: Payload field path.
        """
        ...

    @property
    def key(self) -> str:
        """Field key."""
        ...


class HasIdCondition:
    """Check if point ID is in a set."""

    def __init__(self, point_ids: Set[PointId]) -> None:
        """
        Create a HasIdCondition.

        Args:
            point_ids: Set of point IDs.
        """
        ...

    @property
    def point_ids(self) -> Set[PointId]:
        """Point IDs."""
        ...


class HasVectorCondition:
    """Check if point has a specific vector."""

    def __init__(self, vector: str) -> None:
        """
        Create a HasVectorCondition.

        Args:
            vector: Vector name.
        """
        ...

    @property
    def vector(self) -> str:
        """Vector name."""
        ...


class NestedCondition:
    """Condition on nested objects."""

    def __init__(self, key: JsonPath, filter: Filter) -> None:
        """
        Create a NestedCondition.

        Args:
            key: Path to nested array.
            filter: Filter to apply to nested objects.
        """
        ...

    @property
    def key(self) -> str:
        """Nested field key."""
        ...

    @property
    def filter(self) -> Filter:
        """Nested filter."""
        ...


# ============================================================================
# Match Conditions
# ============================================================================


class MatchValue:
    """Match exact value."""

    def __init__(self, value: Union[str, int, bool]) -> None:
        """
        Create a MatchValue.

        Args:
            value: Value to match.
        """
        ...

    @property
    def value(self) -> Union[str, int, bool]:
        """Value."""
        ...


class MatchText:
    """Full-text match."""

    def __init__(self, text: str) -> None:
        """
        Create a MatchText.

        Args:
            text: Text to search for.
        """
        ...

    @property
    def text(self) -> str:
        """Text."""
        ...


class MatchTextAny:
    """Match any of the words in text."""

    def __init__(self, text_any: str) -> None:
        """
        Create a MatchTextAny.

        Args:
            text_any: Space-separated words to match any of.
        """
        ...

    @property
    def text_any(self) -> str:
        """Text."""
        ...


class MatchPhrase:
    """Match exact phrase."""

    def __init__(self, phrase: str) -> None:
        """
        Create a MatchPhrase.

        Args:
            phrase: Phrase to match.
        """
        ...

    @property
    def phrase(self) -> str:
        """Phrase."""
        ...


class MatchAny:
    """Match any of the values."""

    def __init__(self, any: Union[List[str], List[int]]) -> None:
        """
        Create a MatchAny.

        Args:
            any: List of values to match any of.
        """
        ...

    @property
    def value(self) -> Union[List[str], List[int]]:
        """Values."""
        ...


class MatchExcept:
    """Match any value except these."""

    def __init__(self, except_: Union[List[str], List[int]]) -> None:
        """
        Create a MatchExcept.

        Args:
            except_: List of values to exclude.
        """
        ...

    @property
    def value(self) -> Union[List[str], List[int]]:
        """Excluded values."""
        ...


# ============================================================================
# Range Conditions
# ============================================================================


class RangeFloat:
    """Range condition for float values."""

    def __init__(
            self,
            gte: Optional[float] = None,
            gt: Optional[float] = None,
            lte: Optional[float] = None,
            lt: Optional[float] = None,
    ) -> None:
        """
        Create a RangeFloat.

        Args:
            gte: Greater than or equal.
            gt: Greater than.
            lte: Less than or equal.
            lt: Less than.
        """
        ...

    @property
    def gte(self) -> Optional[float]:
        """Greater than or equal."""
        ...

    @property
    def gt(self) -> Optional[float]:
        """Greater than."""
        ...

    @property
    def lte(self) -> Optional[float]:
        """Less than or equal."""
        ...

    @property
    def lt(self) -> Optional[float]:
        """Less than."""
        ...


class RangeDateTime:
    """Range condition for datetime values."""

    def __init__(
            self,
            gte: Optional[str] = None,
            gt: Optional[str] = None,
            lte: Optional[str] = None,
            lt: Optional[str] = None,
    ) -> None:
        """
        Create a RangeDateTime.

        Args:
            gte: Greater than or equal (ISO 8601 string).
            gt: Greater than (ISO 8601 string).
            lte: Less than or equal (ISO 8601 string).
            lt: Less than (ISO 8601 string).
        """
        ...

    @property
    def gte(self) -> Optional[str]:
        """Greater than or equal."""
        ...

    @property
    def gt(self) -> Optional[str]:
        """Greater than."""
        ...

    @property
    def lte(self) -> Optional[str]:
        """Less than or equal."""
        ...

    @property
    def lt(self) -> Optional[str]:
        """Less than."""
        ...


class ValuesCount:
    """Condition on count of values in array field."""

    def __init__(
            self,
            lt: Optional[int] = None,
            gt: Optional[int] = None,
            lte: Optional[int] = None,
            gte: Optional[int] = None,
    ) -> None:
        """
        Create a ValuesCount.

        Args:
            lt: Less than.
            gt: Greater than.
            lte: Less than or equal.
            gte: Greater than or equal.
        """
        ...

    @property
    def lt(self) -> Optional[int]:
        """Less than."""
        ...

    @property
    def gt(self) -> Optional[int]:
        """Greater than."""
        ...

    @property
    def lte(self) -> Optional[int]:
        """Less than or equal."""
        ...

    @property
    def gte(self) -> Optional[int]:
        """Greater than or equal."""
        ...


# ============================================================================
# Geo Types
# ============================================================================


class GeoPoint:
    """A geographic point."""

    def __init__(self, lon: float, lat: float) -> None:
        """
        Create a GeoPoint.

        Args:
            lon: Longitude (-180 to 180).
            lat: Latitude (-90 to 90).
        """
        ...

    @property
    def lon(self) -> float:
        """Longitude."""
        ...

    @property
    def lat(self) -> float:
        """Latitude."""
        ...


class GeoBoundingBox:
    """A geographic bounding box."""

    def __init__(self, top_left: GeoPoint, bottom_right: GeoPoint) -> None:
        """
        Create a GeoBoundingBox.

        Args:
            top_left: Top-left corner.
            bottom_right: Bottom-right corner.
        """
        ...

    @property
    def top_left(self) -> GeoPoint:
        """Top-left corner."""
        ...

    @property
    def bottom_right(self) -> GeoPoint:
        """Bottom-right corner."""
        ...


class GeoRadius:
    """A geographic circle."""

    def __init__(self, center: GeoPoint, radius: float) -> None:
        """
        Create a GeoRadius.

        Args:
            center: Center point.
            radius: Radius in meters.
        """
        ...

    @property
    def center(self) -> GeoPoint:
        """Center point."""
        ...

    @property
    def radius(self) -> float:
        """Radius in meters."""
        ...


class GeoPolygon:
    """A geographic polygon."""

    def __init__(
            self,
            exterior: List[GeoPoint],
            interiors: Optional[List[List[GeoPoint]]] = None,
    ) -> None:
        """
        Create a GeoPolygon.

        Args:
            exterior: Exterior ring points.
            interiors: Optional interior rings (holes).
        """
        ...

    @property
    def exterior(self) -> List[GeoPoint]:
        """Exterior ring."""
        ...

    @property
    def interiors(self) -> Optional[List[List[GeoPoint]]]:
        """Interior rings (holes)."""
        ...


# ============================================================================
# Payload Selector
# ============================================================================


class PayloadSelector(Enum):
    """Select specific payload fields."""

    @staticmethod
    def Include(keys: List[str]) -> "PayloadSelector":
        """Include only specified fields."""
        ...

    @staticmethod
    def Exclude(keys: List[str]) -> "PayloadSelector":
        """Exclude specified fields."""
        ...


# ============================================================================
# Update Operation
# ============================================================================


class UpdateOperation:
    """Operations for updating shard data."""

    @staticmethod
    def upsert_points(
            points: List[Point],
            condition: Optional[Filter] = None,
            update_mode: Optional[UpdateMode] = None,
    ) -> "UpdateOperation":
        """
        Insert or update points.

        Args:
            points: Points to upsert.
            condition: Optional condition for conditional upsert.
            update_mode: Optional mode of the upsert operation:
                - UpdateMode.Upsert (default): insert new points, update existing points
                - UpdateMode.InsertOnly: only insert new points, do not update existing points
                - UpdateMode.UpdateOnly: only update existing points, do not insert new points
        """
        ...

    @staticmethod
    def delete_points(point_ids: List[PointId]) -> "UpdateOperation":
        """
        Delete points by ID.

        Args:
            point_ids: IDs of points to delete.
        """
        ...

    @staticmethod
    def delete_points_by_filter(filter: Filter) -> "UpdateOperation":
        """
        Delete points matching a filter.

        Args:
            filter: Filter for points to delete.
        """
        ...

    @staticmethod
    def update_vectors(
            point_vectors: List[PointVectors],
            condition: Optional[Filter] = None,
    ) -> "UpdateOperation":
        """
        Update vectors of existing points.

        Args:
            point_vectors: Point IDs with new vectors.
            condition: Optional filter condition.
        """
        ...

    @staticmethod
    def delete_vectors(
            point_ids: List[PointId],
            vector_names: List[str],
    ) -> "UpdateOperation":
        """
        Delete specific vectors from points.

        Args:
            point_ids: Point IDs.
            vector_names: Names of vectors to delete.
        """
        ...

    @staticmethod
    def delete_vectors_by_filter(
            filter: Filter,
            vector_names: List[str],
    ) -> "UpdateOperation":
        """
        Delete vectors from points matching a filter.

        Args:
            filter: Filter for points.
            vector_names: Names of vectors to delete.
        """
        ...

    @staticmethod
    def set_payload(
            point_ids: List[PointId],
            payload: Payload,
            key: Optional[str] = None,
    ) -> "UpdateOperation":
        """
        Set payload fields on points.

        Args:
            point_ids: Point IDs.
            payload: Payload to set.
            key: Optional nested key path.
        """
        ...

    @staticmethod
    def set_payload_by_filter(
            filter: Filter,
            payload: Payload,
            key: Optional[str] = None,
    ) -> "UpdateOperation":
        """
        Set payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: Payload to set.
            key: Optional nested key path.
        """
        ...

    @staticmethod
    def delete_payload(
            point_ids: List[PointId],
            keys: List[str],
    ) -> "UpdateOperation":
        """
        Delete payload fields from points.

        Args:
            point_ids: Point IDs.
            keys: Payload field keys to delete.
        """
        ...

    @staticmethod
    def delete_payload_by_filter(
            filter: Filter,
            keys: List[str],
    ) -> "UpdateOperation":
        """
        Delete payload fields from points matching a filter.

        Args:
            filter: Filter for points.
            keys: Payload field keys to delete.
        """
        ...

    @staticmethod
    def clear_payload(point_ids: List[PointId]) -> "UpdateOperation":
        """
        Clear all payload from points.

        Args:
            point_ids: Point IDs.
        """
        ...

    @staticmethod
    def clear_payload_by_filter(filter: Filter) -> "UpdateOperation":
        """
        Clear all payload from points matching a filter.

        Args:
            filter: Filter for points.
        """
        ...

    @staticmethod
    def overwrite_payload(
            point_ids: List[PointId],
            payload: Payload,
            key: Optional[str] = None,
    ) -> "UpdateOperation":
        """
        Overwrite entire payload on points.

        Args:
            point_ids: Point IDs.
            payload: New payload.
            key: Optional nested key path.
        """
        ...

    @staticmethod
    def overwrite_payload_by_filter(
            filter: Filter,
            payload: Payload,
            key: Optional[str] = None,
    ) -> "UpdateOperation":
        """
        Overwrite payload on points matching a filter.

        Args:
            filter: Filter for points.
            payload: New payload.
            key: Optional nested key path.
        """
        ...
