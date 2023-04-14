use std::collections::BTreeMap;
use std::fmt::Debug;

use api::grpc::models::ApiResponse;
use serde;
use serde::{Deserialize, Serialize};
use storage::types::ClusterStatus;
use utoipa::openapi::response::ResponseExt;
use utoipa::openapi::{
    schema, ContentBuilder, KnownFormat, Object, ObjectBuilder, Ref, RefOr, ResponseBuilder,
    ResponsesBuilder, Schema, SchemaFormat, SchemaType,
};
use utoipa::{schema, IntoResponses, OpenApi, PartialSchema, ToResponse, ToSchema};

#[derive(OpenApi)]
#[openapi(
        paths(
            super::api::cluster_api::cluster_status,
        ),
        components(
            schemas(
                ErrorResponse,
                ClusterStatus,
            ),
        ),
        info(
            title = "Qdrant API",
            contact(email ="andrey@vasnetsov.com"),
            license(name = "Apache 2.0", url = "http://www.apache.org/licenses/LICENSE-2.0.html"),
            version= "master",     
            description = r#"API description for Qdrant vector search engine.


This document describes CRUD and search operations on collections of points (vectors with payload).


Qdrant supports any combinations of `should`, `must` and `must_not` conditions,
which makes it possible to use in applications when object could not be described solely by vector.
It could be location features, availability flags, and other custom properties businesses should take into account.

## Examples

This examples cover the most basic use-cases - collection creation and basic vector search.

### Create collection

First - let's create a collection with dot-production metric.

```

curl -X PUT 'http://localhost:6333/collections/test_collection' \
-H 'Content-Type: application/json' \
--data-raw '{
    "vectors": {
    "size": 4,
    "distance": "Dot"
    }
}'

```

Expected response:

```

{
    "result": true,
    "status": "ok",
    "time": 0.031095451
}

```

We can ensure that collection was created:

```

curl 'http://localhost:6333/collections/test_collection'

```

Expected response:

```

{
"result": {
    "status": "green",
    "vectors_count": 0,
    "segments_count": 5,
    "disk_data_size": 0,
    "ram_data_size": 0,
    "config": {
    "params": {
        "vectors": {
        "size": 4,
        "distance": "Dot"
        }
    },
    "hnsw_config": {
        "m": 16,
        "ef_construct": 100,
        "full_scan_threshold": 10000
    },
    "optimizer_config": {
        "deleted_threshold": 0.2,
        "vacuum_min_vector_number": 1000,
        "max_segment_number": 5,
        "memmap_threshold": 50000,
        "indexing_threshold": 20000,
        "flush_interval_sec": 1
    },
    "wal_config": {
        "wal_capacity_mb": 32,
        "wal_segments_ahead": 0
    }
    }
},
"status": "ok",
"time": 2.1199e-05
}

```


### Add points

Let's now add vectors with some payload:

```

curl -L -X PUT 'http://localhost:6333/collections/test_collection/points?wait=true' \
-H 'Content-Type: application/json' \
--data-raw '{
"points": [
    {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city": "Berlin"}},
    {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": ["Berlin", "London"] }},
    {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": ["Berlin", "Moscow"] }},
    {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": ["London", "Moscow"] }},
    {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": [0]}},
    {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
]
}'

```

Expected response:

```

{
    "result": {
        "operation_id": 0,
        "status": "completed"
    },
    "status": "ok",
    "time": 0.000206061
}

```

### Search with filtering

Let's start with a basic request:

```

curl -L -X POST 'http://localhost:6333/collections/test_collection/points/search' \
-H 'Content-Type: application/json' \
--data-raw '{
    "vector": [0.2,0.1,0.9,0.7],
    "top": 3
}'

```

Expected response:

```

{
    "result": [
        { "id": 4, "score": 1.362, "payload": null, "version": 0 },
        { "id": 1, "score": 1.273, "payload": null, "version": 0 },
        { "id": 3, "score": 1.208, "payload": null, "version": 0 }
    ],
    "status": "ok",
    "time": 0.000055785
}

```

But result is different if we add a filter:

```

curl -L -X POST 'http://localhost:6333/collections/test_collection/points/search' \
-H 'Content-Type: application/json' \
--data-raw '{
    "filter": {
        "should": [
            {
                "key": "city",
                "match": {
                    "value": "London"
                }
            }
        ]
    },
    "vector": [0.2, 0.1, 0.9, 0.7],
    "top": 3
}'

```

Expected response:

```

{
    "result": [
        { "id": 4, "score": 1.362, "payload": null, "version": 0 },
        { "id": 2, "score": 0.871, "payload": null, "version": 0 }
    ],
    "status": "ok",
    "time": 0.000093972
}

```
                "#,
        ),
        external_docs(
            description = "Find out more about Qdrant applications and demo",
            url = "https://qdrant.tech/documentation/",
        ),
        servers(
            (
                url = "{protocol}://{hostname}:{port}",
                variables(
                    (
                        "protocol" = (
                            enum_values(
                                "http",
                                "https"
                            ),
                            default = "http"
                        )
                    ),
                    (
                        "hostname" = (
                            default = "localhost"
                        )
                    ),
                    (
                        "port" = (
                            default = "6333"
                        )
                    ),
                ),
            ),
        ),
        tags(
            (
                name = "collections",
                description = "Searchable collections of points.",
            ),
            (
                name = "points",
                description = "Float-point vectors with payload.",
            ),
            (
                name = "cluster",
                description = "Service distributed setup",
            ),
            (
                name = "snapshots",
                description = "Storage and collections snapshots",
            )
        )
    )]
pub(crate) struct ApiDoc;

pub struct Responses<'__s, T: ToSchema<'__s>>(pub &'__s T);

impl<'__s, T: ToSchema<'__s>> IntoResponses for Responses<'__s, T> {
    fn responses() -> BTreeMap<String, RefOr<utoipa::openapi::response::Response>> {
        let time = ObjectBuilder::new()
            .description(Some("Time spent to process this request"))
            .schema_type(SchemaType::Number)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Float)));

        let success_content = ContentBuilder::new()
            .schema(Schema::Object(
                ObjectBuilder::new()
                    .property("result", Ref::from_schema_name(T::schema().0))
                    .property("status", SuccessfulStatus::schema().1)
                    .property("time", time)
                    .build(),
            ))
            .build();

        ResponsesBuilder::new()
            .response(
                "default",
                ResponseBuilder::new()
                    .description("error")
                    .json_schema_ref(ErrorResponse::schema().0),
            )
            .response(
                "200",
                ResponseBuilder::new()
                    .description("successful operation")
                    .content("application/json", success_content),
            )
            .response(
                "4XX",
                ResponseBuilder::new()
                    .description("error")
                    .json_schema_ref(ErrorResponse::schema().0),
            )
            .build()
            .into()
    }
}

struct ErrorResponse;
impl<'s> ToSchema<'s> for ErrorResponse {
    fn schema() -> (&'s str, RefOr<Schema>) {
        let time = ObjectBuilder::new()
            .description(Some("Time spent to process this request"))
            .schema_type(SchemaType::Number)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Float)));

        let status = ObjectBuilder::new()
            .schema_type(SchemaType::Object)
            .property(
                "error",
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .description(Some("Description of the occurred error.")),
            );

        (
            "ErrorResponse",
            ObjectBuilder::new()
                .property("status", status)
                .property("time", time)
                .property(
                    "result",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Object)
                        .nullable(true),
                )
                .into(),
        )
    }
}
// pub struct ResponseWithAccepted<'__s, T: ToSchema<'__s>>(pub &'__s T);
// impl<'__s, T: ToSchema<'__s>> IntoResponses for ResponseWithAccepted<'__s, T> {
//     fn responses() -> BTreeMap<String, RefOr<utoipa::openapi::response::Response>> {
//         ResponsesBuilder::new()
//             .response(
//                 "default",
//                 ResponseBuilder::new()
//                     .description("error")
//                     .json_schema_ref("ErrorResponse")
//             )
//             .response(
//                 "4XX",
//                 ResponseBuilder::new()
//                     .description("error")
//                     .json_schema_ref("ErrorResponse")
//             )
//             .response(
//                 "200",
//                 ResponseBuilder::new()
//                     .description("successful operation")
//                     .content("application/json",
//                         ContentBuilder::new()
//                             .schema(SuccessfulOperation::<T>::schema().1)
//                             .build()
//                     )
//             )
//             .response(
//                 "202",
//                 ResponseBuilder::new()
//                     .description("operation is accepted")
//                     .content("application/json",
//                         ContentBuilder::new().schema(
//                             ObjectBuilder::new()
//                                 .property("time",
//                                     Schema::Object(ObjectBuilder::new()
//                                         .schema_type(SchemaType::Number)
//                                         .format(Some(SchemaFormat::KnownFormat(KnownFormat::Float)))
//                                         .description(Some("Time spent to process this request"))
//                                         .into())
//                                 )
//                                 .property("status",
//                                     Schema::Object(ObjectBuilder::new()
//                                         .schema_type(SchemaType::String)
//                                         .enum_values(vec!["accepted"].into())
//                                         .into())
//                                 )
//                             ).into()

//                     )
//             )
//             .build()
//             .into()
//     }
// }

#[derive(ToSchema)]
#[schema(rename_all = "snake_case")]
pub enum SuccessfulStatus {
    Ok,
}

// #[derive(ToSchema)]
// #[schema(rename_all = "snake_case")]
// pub enum AcceptedStatus {
//     Accepted,
// }
