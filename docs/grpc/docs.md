# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [collections.proto](#collections-proto)
    - [CollectionConfig](#qdrant-CollectionConfig)
    - [CollectionDescription](#qdrant-CollectionDescription)
    - [CollectionInfo](#qdrant-CollectionInfo)
    - [CollectionInfo.PayloadSchemaEntry](#qdrant-CollectionInfo-PayloadSchemaEntry)
    - [CollectionOperationResponse](#qdrant-CollectionOperationResponse)
    - [CollectionParams](#qdrant-CollectionParams)
    - [CreateCollection](#qdrant-CreateCollection)
    - [DeleteCollection](#qdrant-DeleteCollection)
    - [GetCollectionInfoRequest](#qdrant-GetCollectionInfoRequest)
    - [GetCollectionInfoResponse](#qdrant-GetCollectionInfoResponse)
    - [HnswConfigDiff](#qdrant-HnswConfigDiff)
    - [ListCollectionsRequest](#qdrant-ListCollectionsRequest)
    - [ListCollectionsResponse](#qdrant-ListCollectionsResponse)
    - [OptimizerStatus](#qdrant-OptimizerStatus)
    - [OptimizersConfigDiff](#qdrant-OptimizersConfigDiff)
    - [PayloadSchemaInfo](#qdrant-PayloadSchemaInfo)
    - [UpdateCollection](#qdrant-UpdateCollection)
    - [WalConfigDiff](#qdrant-WalConfigDiff)
  
    - [CollectionStatus](#qdrant-CollectionStatus)
    - [Distance](#qdrant-Distance)
    - [PayloadSchemaType](#qdrant-PayloadSchemaType)
  
    - [Collections](#qdrant-Collections)
  
- [points.proto](#points-proto)
    - [ClearPayloadPoints](#qdrant-ClearPayloadPoints)
    - [Condition](#qdrant-Condition)
    - [CreateFieldIndexCollection](#qdrant-CreateFieldIndexCollection)
    - [DeleteFieldIndexCollection](#qdrant-DeleteFieldIndexCollection)
    - [DeletePayloadPoints](#qdrant-DeletePayloadPoints)
    - [DeletePoints](#qdrant-DeletePoints)
    - [FieldCondition](#qdrant-FieldCondition)
    - [Filter](#qdrant-Filter)
    - [GeoBoundingBox](#qdrant-GeoBoundingBox)
    - [GeoPoint](#qdrant-GeoPoint)
    - [GeoRadius](#qdrant-GeoRadius)
    - [GetPoints](#qdrant-GetPoints)
    - [GetResponse](#qdrant-GetResponse)
    - [HasIdCondition](#qdrant-HasIdCondition)
    - [Match](#qdrant-Match)
    - [PayloadExcludeSelector](#qdrant-PayloadExcludeSelector)
    - [PayloadIncludeSelector](#qdrant-PayloadIncludeSelector)
    - [PointId](#qdrant-PointId)
    - [PointStruct](#qdrant-PointStruct)
    - [PointStruct.PayloadEntry](#qdrant-PointStruct-PayloadEntry)
    - [PointsIdsList](#qdrant-PointsIdsList)
    - [PointsOperationResponse](#qdrant-PointsOperationResponse)
    - [PointsSelector](#qdrant-PointsSelector)
    - [Range](#qdrant-Range)
    - [RecommendPoints](#qdrant-RecommendPoints)
    - [RecommendResponse](#qdrant-RecommendResponse)
    - [RetrievedPoint](#qdrant-RetrievedPoint)
    - [RetrievedPoint.PayloadEntry](#qdrant-RetrievedPoint-PayloadEntry)
    - [ScoredPoint](#qdrant-ScoredPoint)
    - [ScoredPoint.PayloadEntry](#qdrant-ScoredPoint-PayloadEntry)
    - [ScrollPoints](#qdrant-ScrollPoints)
    - [ScrollResponse](#qdrant-ScrollResponse)
    - [SearchParams](#qdrant-SearchParams)
    - [SearchPoints](#qdrant-SearchPoints)
    - [SearchResponse](#qdrant-SearchResponse)
    - [SetPayloadPoints](#qdrant-SetPayloadPoints)
    - [SetPayloadPoints.PayloadEntry](#qdrant-SetPayloadPoints-PayloadEntry)
    - [UpdateResult](#qdrant-UpdateResult)
    - [UpsertPoints](#qdrant-UpsertPoints)
    - [WithPayloadSelector](#qdrant-WithPayloadSelector)
  
    - [FieldType](#qdrant-FieldType)
    - [UpdateStatus](#qdrant-UpdateStatus)
  
    - [Points](#qdrant-Points)
  
- [qdrant.proto](#qdrant-proto)
    - [HealthCheckReply](#qdrant-HealthCheckReply)
    - [HealthCheckRequest](#qdrant-HealthCheckRequest)
  
    - [Qdrant](#qdrant-Qdrant)
  
- [Scalar Value Types](#scalar-value-types)



<a name="collections-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## collections.proto



<a name="qdrant-CollectionConfig"></a>

### CollectionConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| params | [CollectionParams](#qdrant-CollectionParams) |  | Collection parameters |
| hnsw_config | [HnswConfigDiff](#qdrant-HnswConfigDiff) |  | Configuration of vector index |
| optimizer_config | [OptimizersConfigDiff](#qdrant-OptimizersConfigDiff) |  | Configuration of the optimizers |
| wal_config | [WalConfigDiff](#qdrant-WalConfigDiff) |  | Configuration of the Write-Ahead-Log |






<a name="qdrant-CollectionDescription"></a>

### CollectionDescription



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the collection |






<a name="qdrant-CollectionInfo"></a>

### CollectionInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [CollectionStatus](#qdrant-CollectionStatus) |  | operating condition of the collection |
| optimizer_status | [OptimizerStatus](#qdrant-OptimizerStatus) |  | status of collection optimizers |
| vectors_count | [uint64](#uint64) |  | number of vectors in the collection |
| segments_count | [uint64](#uint64) |  | Number of independent segments |
| disk_data_size | [uint64](#uint64) |  | Used disk space |
| ram_data_size | [uint64](#uint64) |  | Used RAM (not implemented) |
| config | [CollectionConfig](#qdrant-CollectionConfig) |  | Configuration |
| payload_schema | [CollectionInfo.PayloadSchemaEntry](#qdrant-CollectionInfo-PayloadSchemaEntry) | repeated | Collection data types |






<a name="qdrant-CollectionInfo-PayloadSchemaEntry"></a>

### CollectionInfo.PayloadSchemaEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [PayloadSchemaInfo](#qdrant-PayloadSchemaInfo) |  |  |






<a name="qdrant-CollectionOperationResponse"></a>

### CollectionOperationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [bool](#bool) |  | if operation made changes |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-CollectionParams"></a>

### CollectionParams



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| vector_size | [uint64](#uint64) |  | Size of the vectors |
| distance | [Distance](#qdrant-Distance) |  | Distance function used for comparing vectors |
| shard_number | [uint32](#uint32) |  | Number of shards in collection |






<a name="qdrant-CreateCollection"></a>

### CreateCollection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | Name of the collection |
| vector_size | [uint64](#uint64) |  | Size of the vectors |
| distance | [Distance](#qdrant-Distance) |  | Distance function used for comparing vectors |
| hnsw_config | [HnswConfigDiff](#qdrant-HnswConfigDiff) | optional | Configuration of vector index |
| wal_config | [WalConfigDiff](#qdrant-WalConfigDiff) | optional | Configuration of the Write-Ahead-Log |
| optimizers_config | [OptimizersConfigDiff](#qdrant-OptimizersConfigDiff) | optional | Configuration of the optimizers |
| shard_number | [uint32](#uint32) | optional | Number of shards in the collection, default = 1 |






<a name="qdrant-DeleteCollection"></a>

### DeleteCollection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | Name of the collection |






<a name="qdrant-GetCollectionInfoRequest"></a>

### GetCollectionInfoRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | Name of the collection |






<a name="qdrant-GetCollectionInfoResponse"></a>

### GetCollectionInfoResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [CollectionInfo](#qdrant-CollectionInfo) |  |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-HnswConfigDiff"></a>

### HnswConfigDiff



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| m | [uint64](#uint64) | optional | Number of edges per node in the index graph. Larger the value - more accurate the search, more space required. |
| ef_construct | [uint64](#uint64) | optional | Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index. |
| full_scan_threshold | [uint64](#uint64) | optional | Minimal amount of points for additional payload-based indexing. If payload chunk is smaller than `full_scan_threshold` additional indexing won&#39;t be used - in this case full-scan search should be preferred by query planner and additional indexing is not required. |






<a name="qdrant-ListCollectionsRequest"></a>

### ListCollectionsRequest







<a name="qdrant-ListCollectionsResponse"></a>

### ListCollectionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collections | [CollectionDescription](#qdrant-CollectionDescription) | repeated |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-OptimizerStatus"></a>

### OptimizerStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |
| error | [string](#string) |  |  |






<a name="qdrant-OptimizersConfigDiff"></a>

### OptimizersConfigDiff



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted_threshold | [double](#double) | optional | The minimal fraction of deleted vectors in a segment, required to perform segment optimization |
| vacuum_min_vector_number | [uint64](#uint64) | optional | The minimal number of vectors in a segment, required to perform segment optimization |
| default_segment_number | [uint64](#uint64) | optional | Target amount of segments optimizer will try to keep. Real amount of segments may vary depending on multiple parameters:

- Amount of stored points. - Current write RPS.

It is recommended to select default number of segments as a factor of the number of search threads, so that each segment would be handled evenly by one of the threads. |
| max_segment_size | [uint64](#uint64) | optional | Do not create segments larger this number of points. Large segments might require disproportionately long indexation times, therefore it makes sense to limit the size of segments.

If indexation speed have more priority for your - make this parameter lower. If search speed is more important - make this parameter higher. |
| memmap_threshold | [uint64](#uint64) | optional | Maximum number of vectors to store in-memory per segment. Segments larger than this threshold will be stored as read-only memmaped file. |
| indexing_threshold | [uint64](#uint64) | optional | Maximum number of vectors allowed for plain index. Default value based on https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md |
| payload_indexing_threshold | [uint64](#uint64) | optional | Starting from this amount of vectors per-segment the engine will start building index for payload. |
| flush_interval_sec | [uint64](#uint64) | optional | Interval between forced flushes. |
| max_optimization_threads | [uint64](#uint64) | optional | Max number of threads, which can be used for optimization. If 0 - `NUM_CPU - 1` will be used |






<a name="qdrant-PayloadSchemaInfo"></a>

### PayloadSchemaInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_type | [PayloadSchemaType](#qdrant-PayloadSchemaType) |  | Field data type |
| indexed | [bool](#bool) |  | If this field is indexed |






<a name="qdrant-UpdateCollection"></a>

### UpdateCollection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | Name of the collection |
| optimizers_config | [OptimizersConfigDiff](#qdrant-OptimizersConfigDiff) | optional | New configuration parameters for the collection |






<a name="qdrant-WalConfigDiff"></a>

### WalConfigDiff



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| wal_capacity_mb | [uint64](#uint64) | optional | Size of a single WAL block file |
| wal_segments_ahead | [uint64](#uint64) | optional | Number of segments to create in advance |





 


<a name="qdrant-CollectionStatus"></a>

### CollectionStatus


| Name | Number | Description |
| ---- | ------ | ----------- |
| UnknownCollectionStatus | 0 |  |
| Green | 1 | All segments are ready |
| Yellow | 2 | Optimization in process |
| Red | 3 | Something went wrong |



<a name="qdrant-Distance"></a>

### Distance


| Name | Number | Description |
| ---- | ------ | ----------- |
| UnknownDistance | 0 |  |
| Cosine | 1 |  |
| Euclid | 2 |  |
| Dot | 3 |  |



<a name="qdrant-PayloadSchemaType"></a>

### PayloadSchemaType


| Name | Number | Description |
| ---- | ------ | ----------- |
| UnknownType | 0 |  |
| Keyword | 1 |  |
| Integer | 2 |  |
| Float | 3 |  |
| Geo | 4 |  |


 

 


<a name="qdrant-Collections"></a>

### Collections


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Get | [GetCollectionInfoRequest](#qdrant-GetCollectionInfoRequest) | [GetCollectionInfoResponse](#qdrant-GetCollectionInfoResponse) | Get detailed information about specified existing collection |
| List | [ListCollectionsRequest](#qdrant-ListCollectionsRequest) | [ListCollectionsResponse](#qdrant-ListCollectionsResponse) | Get list name of all existing collections |
| Create | [CreateCollection](#qdrant-CreateCollection) | [CollectionOperationResponse](#qdrant-CollectionOperationResponse) | Create new collection with given parameters |
| Update | [UpdateCollection](#qdrant-UpdateCollection) | [CollectionOperationResponse](#qdrant-CollectionOperationResponse) | Update parameters of the existing collection |
| Delete | [DeleteCollection](#qdrant-DeleteCollection) | [CollectionOperationResponse](#qdrant-CollectionOperationResponse) | Drop collection and all associated data |

 



<a name="points-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## points.proto



<a name="qdrant-ClearPayloadPoints"></a>

### ClearPayloadPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| points | [PointsSelector](#qdrant-PointsSelector) |  | Affected points |






<a name="qdrant-Condition"></a>

### Condition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field | [FieldCondition](#qdrant-FieldCondition) |  |  |
| hasId | [HasIdCondition](#qdrant-HasIdCondition) |  |  |
| filter | [Filter](#qdrant-Filter) |  |  |






<a name="qdrant-CreateFieldIndexCollection"></a>

### CreateFieldIndexCollection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| field_name | [string](#string) |  | Field name to index |
| field_type | [FieldType](#qdrant-FieldType) | optional | Field type. |






<a name="qdrant-DeleteFieldIndexCollection"></a>

### DeleteFieldIndexCollection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| field_name | [string](#string) |  | Field name to delete |






<a name="qdrant-DeletePayloadPoints"></a>

### DeletePayloadPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| keys | [string](#string) | repeated | List of keys to delete |
| points | [PointId](#qdrant-PointId) | repeated | Affected points |






<a name="qdrant-DeletePoints"></a>

### DeletePoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| points | [PointsSelector](#qdrant-PointsSelector) |  | Affected points |






<a name="qdrant-FieldCondition"></a>

### FieldCondition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| match | [Match](#qdrant-Match) |  | Check if point has field with a given value |
| range | [Range](#qdrant-Range) |  | Check if points value lies in a given range |
| geo_bounding_box | [GeoBoundingBox](#qdrant-GeoBoundingBox) |  | Check if points geo location lies in a given area |
| geo_radius | [GeoRadius](#qdrant-GeoRadius) |  | Check if geo point is within a given radius |






<a name="qdrant-Filter"></a>

### Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| should | [Condition](#qdrant-Condition) | repeated | At least one of those conditions should match |
| must | [Condition](#qdrant-Condition) | repeated | All conditions must match |
| must_not | [Condition](#qdrant-Condition) | repeated | All conditions must NOT match |






<a name="qdrant-GeoBoundingBox"></a>

### GeoBoundingBox



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_left | [GeoPoint](#qdrant-GeoPoint) |  | north-west corner |
| bottom_right | [GeoPoint](#qdrant-GeoPoint) |  | south-east corner |






<a name="qdrant-GeoPoint"></a>

### GeoPoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lon | [double](#double) |  |  |
| lat | [double](#double) |  |  |






<a name="qdrant-GeoRadius"></a>

### GeoRadius



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| center | [GeoPoint](#qdrant-GeoPoint) |  | Center of the circle |
| radius | [float](#float) |  | In meters |






<a name="qdrant-GetPoints"></a>

### GetPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| ids | [PointId](#qdrant-PointId) | repeated | List of points to retrieve |
| with_vector | [bool](#bool) | optional | Return point vector with the result. |
| with_payload | [WithPayloadSelector](#qdrant-WithPayloadSelector) |  | Options for specifying which payload to include or not |






<a name="qdrant-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [RetrievedPoint](#qdrant-RetrievedPoint) | repeated |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-HasIdCondition"></a>

### HasIdCondition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_id | [PointId](#qdrant-PointId) | repeated |  |






<a name="qdrant-Match"></a>

### Match



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keyword | [string](#string) |  | Match string keyword |
| integer | [int64](#int64) |  | Match integer |






<a name="qdrant-PayloadExcludeSelector"></a>

### PayloadExcludeSelector



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exclude | [string](#string) | repeated | List of payload keys to exclude from the result |






<a name="qdrant-PayloadIncludeSelector"></a>

### PayloadIncludeSelector



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| include | [string](#string) | repeated | List of payload keys to include into result |






<a name="qdrant-PointId"></a>

### PointId



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| num | [uint64](#uint64) |  | Numerical ID of the point |
| uuid | [string](#string) |  | UUID |






<a name="qdrant-PointStruct"></a>

### PointStruct



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [PointId](#qdrant-PointId) |  |  |
| vector | [float](#float) | repeated |  |
| payload | [PointStruct.PayloadEntry](#qdrant-PointStruct-PayloadEntry) | repeated |  |






<a name="qdrant-PointStruct-PayloadEntry"></a>

### PointStruct.PayloadEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="qdrant-PointsIdsList"></a>

### PointsIdsList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [PointId](#qdrant-PointId) | repeated |  |






<a name="qdrant-PointsOperationResponse"></a>

### PointsOperationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [UpdateResult](#qdrant-UpdateResult) |  |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-PointsSelector"></a>

### PointsSelector



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| points | [PointsIdsList](#qdrant-PointsIdsList) |  |  |
| filter | [Filter](#qdrant-Filter) |  |  |






<a name="qdrant-Range"></a>

### Range



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lt | [double](#double) | optional |  |
| gt | [double](#double) | optional |  |
| gte | [double](#double) | optional |  |
| lte | [double](#double) | optional |  |






<a name="qdrant-RecommendPoints"></a>

### RecommendPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| positive | [PointId](#qdrant-PointId) | repeated | Look for vectors closest to those |
| negative | [PointId](#qdrant-PointId) | repeated | Try to avoid vectors like this |
| filter | [Filter](#qdrant-Filter) |  | Filter conditions - return only those points that satisfy the specified conditions |
| top | [uint64](#uint64) |  | Max number of result |
| with_vector | [bool](#bool) | optional | Return point vector with the result. |
| with_payload | [WithPayloadSelector](#qdrant-WithPayloadSelector) |  | Options for specifying which payload to include or not |
| params | [SearchParams](#qdrant-SearchParams) |  | Search config |






<a name="qdrant-RecommendResponse"></a>

### RecommendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [ScoredPoint](#qdrant-ScoredPoint) | repeated |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-RetrievedPoint"></a>

### RetrievedPoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [PointId](#qdrant-PointId) |  |  |
| payload | [RetrievedPoint.PayloadEntry](#qdrant-RetrievedPoint-PayloadEntry) | repeated |  |
| vector | [float](#float) | repeated |  |






<a name="qdrant-RetrievedPoint-PayloadEntry"></a>

### RetrievedPoint.PayloadEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="qdrant-ScoredPoint"></a>

### ScoredPoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [PointId](#qdrant-PointId) |  | Point id |
| payload | [ScoredPoint.PayloadEntry](#qdrant-ScoredPoint-PayloadEntry) | repeated | Payload |
| score | [float](#float) |  | Similarity score |
| vector | [float](#float) | repeated | Vector |
| version | [uint64](#uint64) |  | Last update operation applied to this point |






<a name="qdrant-ScoredPoint-PayloadEntry"></a>

### ScoredPoint.PayloadEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="qdrant-ScrollPoints"></a>

### ScrollPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  |  |
| filter | [Filter](#qdrant-Filter) |  | Filter conditions - return only those points that satisfy the specified conditions |
| offset | [PointId](#qdrant-PointId) | optional | Start with this ID |
| limit | [uint32](#uint32) | optional | Max number of result |
| with_vector | [bool](#bool) | optional | Return point vector with the result. |
| with_payload | [WithPayloadSelector](#qdrant-WithPayloadSelector) |  | Options for specifying which payload to include or not |






<a name="qdrant-ScrollResponse"></a>

### ScrollResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| next_page_offset | [PointId](#qdrant-PointId) | optional | Use this offset for the next query |
| result | [RetrievedPoint](#qdrant-RetrievedPoint) | repeated |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-SearchParams"></a>

### SearchParams



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hnsw_ef | [uint64](#uint64) | optional | Params relevant to HNSW index. Size of the beam in a beam-search. Larger the value - more accurate the result, more time required for search. |






<a name="qdrant-SearchPoints"></a>

### SearchPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| vector | [float](#float) | repeated | vector |
| filter | [Filter](#qdrant-Filter) |  | Filter conditions - return only those points that satisfy the specified conditions |
| top | [uint64](#uint64) |  | Max number of result |
| with_vector | [bool](#bool) | optional | Return point vector with the result. |
| with_payload | [WithPayloadSelector](#qdrant-WithPayloadSelector) |  | Options for specifying which payload to include or not |
| params | [SearchParams](#qdrant-SearchParams) |  | Search config |






<a name="qdrant-SearchResponse"></a>

### SearchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [ScoredPoint](#qdrant-ScoredPoint) | repeated |  |
| time | [double](#double) |  | Time spent to process |






<a name="qdrant-SetPayloadPoints"></a>

### SetPayloadPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| payload | [SetPayloadPoints.PayloadEntry](#qdrant-SetPayloadPoints-PayloadEntry) | repeated | New payload values |
| points | [PointId](#qdrant-PointId) | repeated | List of point to modify |






<a name="qdrant-SetPayloadPoints-PayloadEntry"></a>

### SetPayloadPoints.PayloadEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="qdrant-UpdateResult"></a>

### UpdateResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [uint64](#uint64) |  | Number of operation |
| status | [UpdateStatus](#qdrant-UpdateStatus) |  | Operation status |






<a name="qdrant-UpsertPoints"></a>

### UpsertPoints



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection_name | [string](#string) |  | name of the collection |
| wait | [bool](#bool) | optional | Wait until the changes have been applied? |
| points | [PointStruct](#qdrant-PointStruct) | repeated |  |






<a name="qdrant-WithPayloadSelector"></a>

### WithPayloadSelector



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enable | [bool](#bool) |  | If `true` - return all payload, if `false` - none |
| include | [PayloadIncludeSelector](#qdrant-PayloadIncludeSelector) |  |  |
| exclude | [PayloadExcludeSelector](#qdrant-PayloadExcludeSelector) |  |  |





 


<a name="qdrant-FieldType"></a>

### FieldType


| Name | Number | Description |
| ---- | ------ | ----------- |
| FieldTypeKeyword | 0 |  |
| FieldTypeInteger | 1 |  |
| FieldTypeFloat | 2 |  |
| FieldTypeGeo | 3 |  |



<a name="qdrant-UpdateStatus"></a>

### UpdateStatus


| Name | Number | Description |
| ---- | ------ | ----------- |
| UnknownUpdateStatus | 0 |  |
| Acknowledged | 1 | Update is received, but not processed yet |
| Completed | 2 | Update is applied and ready for search |


 

 


<a name="qdrant-Points"></a>

### Points


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Upsert | [UpsertPoints](#qdrant-UpsertPoints) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Perform insert &#43; updates on points. If point with given ID already exists - it will be overwritten. |
| Delete | [DeletePoints](#qdrant-DeletePoints) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Delete points |
| Get | [GetPoints](#qdrant-GetPoints) | [GetResponse](#qdrant-GetResponse) | Retrieve points |
| SetPayload | [SetPayloadPoints](#qdrant-SetPayloadPoints) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Set payload for points |
| DeletePayload | [DeletePayloadPoints](#qdrant-DeletePayloadPoints) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Delete specified key payload for points |
| ClearPayload | [ClearPayloadPoints](#qdrant-ClearPayloadPoints) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Remove all payload for specified points |
| CreateFieldIndex | [CreateFieldIndexCollection](#qdrant-CreateFieldIndexCollection) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Create index for field in collection |
| DeleteFieldIndex | [DeleteFieldIndexCollection](#qdrant-DeleteFieldIndexCollection) | [PointsOperationResponse](#qdrant-PointsOperationResponse) | Delete field index for collection |
| Search | [SearchPoints](#qdrant-SearchPoints) | [SearchResponse](#qdrant-SearchResponse) | Retrieve closest points based on vector similarity and given filtering conditions |
| Scroll | [ScrollPoints](#qdrant-ScrollPoints) | [ScrollResponse](#qdrant-ScrollResponse) | Iterate over all or filtered points points |
| Recommend | [RecommendPoints](#qdrant-RecommendPoints) | [RecommendResponse](#qdrant-RecommendResponse) | Look for the points which are closer to stored positive examples and at the same time further to negative examples. |

 



<a name="qdrant-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## qdrant.proto



<a name="qdrant-HealthCheckReply"></a>

### HealthCheckReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| title | [string](#string) |  |  |
| version | [string](#string) |  |  |






<a name="qdrant-HealthCheckRequest"></a>

### HealthCheckRequest






 

 

 


<a name="qdrant-Qdrant"></a>

### Qdrant


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HealthCheck | [HealthCheckRequest](#qdrant-HealthCheckRequest) | [HealthCheckReply](#qdrant-HealthCheckReply) |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

