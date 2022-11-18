import grpc from 'k6/net/grpc';
import { check, group } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

import { random_city, random_vector } from '/code/utils.js';

// test system parameters
let host = 'localhost:6334'
let collection_name = 'grpc_stress';
let shard_count = 1; // increase in distributed mode
let replica_count = 1; // increase in distributed mode

// test payload parameters
let vector_length = 128;
let points_per_batch = 32;
let number_of_points = 300000;

const client = new grpc.Client();
client.load(['/proto'], 'collections_service.proto', 'points_service.proto');

const pointsCount = new Counter('points_count');

export const options = {
   discardResponseBodies: true, // decrease memory usage
    scenarios: {
        upsert_points: {
            // function to execute
            exec: "upsert_points",
            // execution options
            executor: 'shared-iterations',
            vus: 30, // number of VUs to run concurrently
            iterations: number_of_points / points_per_batch, //total number of iterations
            maxDuration: '10m',
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
    // connect client on first iteration
    // https://github.com/grafana/k6/issues/2719#issuecomment-1280033675
    if (exec.vu.iterationInScenario == 0) {
       client.connect(host, { plaintext: true });
    }

    // points payload
    var payload = {
        collection_name: collection_name,
        points: Array.from({ length: points_per_batch }, () => generate_point()),
    }
    // run upsert
    let res_upsert = client.invoke('qdrant.Points/Upsert', payload);
    check(res_upsert, {
       'upsert points status is OK': (r) => r.status === grpc.StatusOK,
    });

    // track number of points created
    pointsCount.add(points_per_batch);
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