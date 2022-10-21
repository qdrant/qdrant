import http from "k6/http";
import { check, group } from 'k6';
import { Counter } from 'k6/metrics';

// test system parameters
let host = 'http://localhost:6333'
let collection_name = 'stress_collection';
let shard_count = 1; // increase in distributed mode
let replica_count = 1; // increase in distributed mode

// urls
let collection_url = `${host}/collections/${collection_name}`;
let collection_index_url = `${host}/collections/${collection_name}/index`;
let points_url = `${host}/collections/${collection_name}/points`;
let points_search_url = `${host}/collections/${collection_name}/points/search`;

// test payload parameters
let vector_length = 128;
let vectors_per_batch = 32;

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
    },
};

var cities = [
    "Tokyo",
    "Delhi",
    "Shanghai",
    "São Paulo",
    "Mexico City",
    "Cairo",
    "Mumbai",
    "Beijing",
    "Dhaka",
    "Osaka",
    "New York City",
    "Karachi",
    "Buenos Aires",
    "Chongqing",
    "Istanbul",
    "Kolkata",
    "Manila",
    "Lagos",
    "Rio de Janeiro",
    "Tianjin",
    "Kinshasa",
    "Guangzhou",
    "Los Angeles",
    "Moscow",
    "Shenzhen",
    "Lahore",
    "Bangalore",
    "Paris",
    "Bogotá",
    "Jakarta",
    "Chennai",
    "Lima",
    "Bangkok",
    "Seoul",
    "Nagoya",
    "Hyderabad",
    "London",
    "Tehran",
    "Chicago",
    "Chengdu",
    "Nanjing",
    "Wuhan",
    "Ho Chi Minh City",
    "Luanda",
    "Ahmedabad",
    "Kuala Lumpur",
    "Xi'an",
    "Hong Kong",
    "Dongguan",
    "Hangzhou"
]

function random_vector() {
    return Array.from({ length: vector_length }, () => Math.random());
}

function random_city() {
    return cities[Math.round(Math.random()*(cities.length-1))];
}

function generate_point() {
    var idx = Math.floor(Math.random() * 1000000000);
    var count = Math.floor(Math.random() * 100);
    var vector = random_vector();
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
    pointsCount.add(vectors_per_batch);
    // points payload
    var payload = JSON.stringify({
        "points": Array.from({ length: vectors_per_batch }, () => generate_point()),
    });
    // run upsert
    let res_upsert = http.put(points_url, payload, params);
    check(res_upsert, {
        'upsert_points is status 200': (r) => r.status === 200,
    });
}

export function search_points() {
    // generate random search query
    var filter_payload =
        {
            "filter": {
                "must": [
                    {
                        "key": "city",
                        "match": {
                            "value": random_city()
                        }
                    }
                ]
            },
            "with_vector": true,
            "with_payload": true,
            "vector": random_vector(),
            "limit": 10
        }

    let res_get = http.post(points_search_url, JSON.stringify(filter_payload), params);
    //console.log(res_get.body);
    check(res_get, {
        'search_points is status 200': (r) => r.status === 200,
    });
}

export function teardown() {
    console.log('Done');
}