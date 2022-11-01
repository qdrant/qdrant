import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

import { random_city, random_vector } from '/code/utils.js';

// test system parameters
let host = 'localhost:6334'
let collection_name = 'grpc_stress_collection';
let shard_count = 1; // increase in distributed mode
let replica_count = 1; // increase in distributed mode

// test payload parameters
let vector_length = 128;
let vectors_per_batch = 32;

const client = new grpc.Client();
client.load(['/proto'], 'collections_service.proto', 'points_service.proto');

const pointsCount = new Counter('points_count');

export const options = {
    scenarios: {
        upsert_points: {
            // function to execute
            exec: "upsert_points",
            // execution options
            executor: "ramping-vus",
            stages: [{
                duration: '1m', target: 30
            }],
        },
        search_points: {
            // schedule this scenario to start after the upserts (remove for mixed workload)
            startTime: "1m",
            // function to execute
            exec: "search_points",
            // execution options
            executor: "ramping-vus",
            stages: [{
                duration: "1m", target: 30
            }],
        },
    },
};

export function setup() {
    // connect client
    client.connect(host, { plaintext: true });

    // delete collection
    const response_del = client.invoke('qdrant.Collections/Delete', { "collection_name": collection_name });
    check(response_del, {
      'delete collection status is OK': (r) => r.status === grpc.StatusOK,
    });

    // create a new collection
    const response_create = client.invoke('qdrant.Collections/Create', create_collection_payload);
    check(response_create, {
      'create new collection status is OK': (r) => r.status === grpc.StatusOK,
    });

    // add payload index
    const response_index = client.invoke('qdrant.Points/CreateFieldIndex', create_payload_index_payload);
    check(response_index, {
      'add payload index status is OK': (r) => r.status === grpc.StatusOK,
    });

    //console.log(JSON.stringify(response_index.message));

    // close client
    client.close();
}

export function upsert_points() {
    // connect client
    client.connect(host, { plaintext: true });

    // points payload
    var payload = {
        collection_name: collection_name,
        points: Array.from({ length: vectors_per_batch }, () => generate_point()),
    }
    // run upsert
    let res_upsert = client.invoke('qdrant.Points/Upsert', payload);
    check(res_upsert, {
       'upsert points status is OK': (r) => r.status === grpc.StatusOK,
    });

    // track number of points created
    pointsCount.add(vectors_per_batch);

    // close client
    client.close();
}

export function search_points() {
    // connect client
    client.connect(host, { plaintext: true });

    // generate random search query
    var filter_payload =
        {
            collection_name: collection_name,
            filter: {
                must: [
                    {
                        key: "city",
                        match: {
                            value: random_city()
                        }
                    }
                ]
            },
            with_vector: true,
            with_payload: true,
            vector: random_vector(vector_length),
            limit: 100
        }

    let res_search = client.invoke('qdrant.Points/Search', filter_payload);
    //console.log(res_search.body);
    check(res_search, {
        'search_points status is 200': (r) => r.status === grpc.StatusOK,
    });

    // close client
    client.close();
}

function generate_point() {
    var idx = Math.floor(Math.random() * 1000000000);
    var count = Math.floor(Math.random() * 100);
    var vector = random_vector(vector_length);
    var city = random_city();

    return {
        id: { num: idx },
        vectors: { vector: { data: vector }},
        payload: {
            city: { string_value: city },
            count: { integer_value: count },
        }
    }
}

var create_collection_payload =
    {
        collection_name: collection_name,
        vectors_config: {
            params: {
               size: vector_length,
               distance: "Cosine"
            }
        },
        shard_number: shard_count,
        replication_factor: replica_count,
    }

var create_payload_index_payload =
    {
        collection_name: collection_name,
        field_name: "city",
        field_type: 0,
    }