import http from "k6/http";
import { check, group } from 'k6';
import { Counter } from 'k6/metrics';
import { random_city, random_vector } from '/code/utils.js';

// test system parameters
let host = 'http://localhost:6333'
let collection_name = 'rest_stress';
let shard_count = 1; // increase in distributed mode
let replica_count = 1; // increase in distributed mode

// urls
let collection_url = `${host}/collections/${collection_name}`;
let collection_index_url = `${host}/collections/${collection_name}/index`;
let points_url = `${host}/collections/${collection_name}/points`;
let points_search_url = `${host}/collections/${collection_name}/points/search`;

// test payload parameters
let vector_length = 128;
let points_per_batch = 32;
let number_of_points = 300000;

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

const pointsCount = new Counter('points_count');

var create_collection_payload = JSON.stringify(
    {
        "vectors": {
            "size": vector_length,
            "distance": "Cosine"
        },
        "shard_number": shard_count,
        "replication_factor": replica_count,
    }
);

var create_payload_index_payload = JSON.stringify(
    {
        "field_name": "city",
        "field_schema": "keyword"
    }
);

var params = {
    headers: {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
    },
};

function generate_point() {
    var idx = Math.floor(Math.random() * 1000000000);
    var count = Math.floor(Math.random() * 100);
    var vector = random_vector(vector_length);
    var city = random_city();

    return {
        "id": idx,
        "vector": vector,
        "payload": {
            "city": city,
            "count": count
        }
    }
}

export function setup() {
    // delete collection
    let res_delete = http.del(collection_url, params);
    check(res_delete, {
        'delete_collection_payload is status 200': (r) => r.status === 200,
    });

    // create a new collection
    let res_create = http.put(collection_url, create_collection_payload, params);
    check(res_create, {
        'create_collection_payload is status 200': (r) => r.status === 200,
    });

    // add payload index
    let res_index = http.put(collection_index_url, create_payload_index_payload, params);
    check(res_index, {
        'create_index_payload is status 200': (r) => r.status === 200,
    });
}

export function upsert_points() {
    // points payload
    var payload = JSON.stringify({
        "points": Array.from({ length: points_per_batch }, () => generate_point()),
    });
    // run upsert
    let res_upsert = http.put(points_url, payload, params);
    check(res_upsert, {
        'upsert_points is status 200': (r) => r.status === 200,
    });
    // track number of points created
    pointsCount.add(points_per_batch);
}