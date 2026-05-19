@file:Suppress("unused")

package tech.qdrant.edge

// Public API for Qdrant Edge on Android.
//
// Every symbol below is a compile-time typealias onto the UniFFI-generated
// bindings in `:qdrant-edge-ffi`. Consumers import `tech.qdrant.edge.*` and
// never see FFI plumbing (`FfiConverter*`, `RustBuffer`, …).
//
// When a new public type is added to the Rust FFI crate, add a typealias
// here. Types that should stay internal simply don't get an alias.

// ---- Shards & operations --------------------------------------------------

public typealias EdgeShard = tech.qdrant.edge.ffi.EdgeShard
public typealias UpdateOperation = tech.qdrant.edge.ffi.UpdateOperation

// ---- Configs & records ----------------------------------------------------

public typealias BinaryQuantizationParams = tech.qdrant.edge.ffi.BinaryQuantizationParams
public typealias CountRequest = tech.qdrant.edge.ffi.CountRequest
public typealias EdgeConfig = tech.qdrant.edge.ffi.EdgeConfig
public typealias FacetHit = tech.qdrant.edge.ffi.FacetHit
public typealias FacetRequest = tech.qdrant.edge.ffi.FacetRequest
public typealias FacetResponse = tech.qdrant.edge.ffi.FacetResponse
public typealias FieldCondition = tech.qdrant.edge.ffi.FieldCondition
public typealias Filter = tech.qdrant.edge.ffi.Filter
public typealias GeoBoundingBox = tech.qdrant.edge.ffi.GeoBoundingBox
public typealias GeoPoint = tech.qdrant.edge.ffi.GeoPoint
public typealias GeoRadius = tech.qdrant.edge.ffi.GeoRadius
public typealias MultiVectorConfig = tech.qdrant.edge.ffi.MultiVectorConfig
public typealias OrderBy = tech.qdrant.edge.ffi.OrderBy
public typealias Point = tech.qdrant.edge.ffi.Point
public typealias PointVectors = tech.qdrant.edge.ffi.PointVectors
public typealias Prefetch = tech.qdrant.edge.ffi.Prefetch
public typealias ProductQuantizationParams = tech.qdrant.edge.ffi.ProductQuantizationParams
public typealias QueryRequest = tech.qdrant.edge.ffi.QueryRequest
public typealias RangeFloat = tech.qdrant.edge.ffi.RangeFloat
public typealias Record = tech.qdrant.edge.ffi.Record
public typealias ScalarQuantizationParams = tech.qdrant.edge.ffi.ScalarQuantizationParams
public typealias ScoredPoint = tech.qdrant.edge.ffi.ScoredPoint
public typealias ScrollRequest = tech.qdrant.edge.ffi.ScrollRequest
public typealias ScrollResponse = tech.qdrant.edge.ffi.ScrollResponse
public typealias SearchParams = tech.qdrant.edge.ffi.SearchParams
public typealias SearchRequest = tech.qdrant.edge.ffi.SearchRequest
public typealias ShardInfo = tech.qdrant.edge.ffi.ShardInfo
public typealias SparseVector = tech.qdrant.edge.ffi.SparseVector
public typealias SparseVectorDataConfig = tech.qdrant.edge.ffi.SparseVectorDataConfig
public typealias ValuesCount = tech.qdrant.edge.ffi.ValuesCount
public typealias VectorDataConfig = tech.qdrant.edge.ffi.VectorDataConfig

// ---- Enums ----------------------------------------------------------------

public typealias BinaryQuantizationEncoding = tech.qdrant.edge.ffi.BinaryQuantizationEncoding
public typealias BinaryQuantizationQueryEncoding = tech.qdrant.edge.ffi.BinaryQuantizationQueryEncoding
public typealias CompressionRatio = tech.qdrant.edge.ffi.CompressionRatio
public typealias Direction = tech.qdrant.edge.ffi.Direction
public typealias Distance = tech.qdrant.edge.ffi.Distance
public typealias Modifier = tech.qdrant.edge.ffi.Modifier
public typealias MultiVectorComparator = tech.qdrant.edge.ffi.MultiVectorComparator
public typealias Sample = tech.qdrant.edge.ffi.Sample
public typealias ScalarType = tech.qdrant.edge.ffi.ScalarType
public typealias SparseIndexType = tech.qdrant.edge.ffi.SparseIndexType
public typealias UpdateMode = tech.qdrant.edge.ffi.UpdateMode
public typealias VectorStorageDatatype = tech.qdrant.edge.ffi.VectorStorageDatatype

// ---- Sealed unions --------------------------------------------------------

public typealias Condition = tech.qdrant.edge.ffi.Condition
public typealias Fusion = tech.qdrant.edge.ffi.Fusion
public typealias Match = tech.qdrant.edge.ffi.Match
public typealias NamedVector = tech.qdrant.edge.ffi.NamedVector
public typealias PointId = tech.qdrant.edge.ffi.PointId
public typealias QuantizationConfig = tech.qdrant.edge.ffi.QuantizationConfig
public typealias Query = tech.qdrant.edge.ffi.Query
public typealias ScoringQuery = tech.qdrant.edge.ffi.ScoringQuery
public typealias ValueVariants = tech.qdrant.edge.ffi.ValueVariants
public typealias Vector = tech.qdrant.edge.ffi.Vector
public typealias WithPayload = tech.qdrant.edge.ffi.WithPayload
public typealias WithVector = tech.qdrant.edge.ffi.WithVector

// ---- Errors ---------------------------------------------------------------

public typealias EdgeException = tech.qdrant.edge.ffi.EdgeException
