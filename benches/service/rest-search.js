import http from "k6/http";
import { check, group } from 'k6';
import { random_city, random_vector } from '/code/utils.js';

// test system parameters
let host = 'http://localhost:6333'
let collection_name = 'rest_stress';

// urls
let points_search_url = `${host}/collections/${collection_name}/points/search`;

// test payload parameters
let vector_length = 128;

export const options = {
    discardResponseBodies: true, // decrease memory usage
    scenarios: {
        search_points: {
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

var params = {
    headers: {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
    },
};

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
            "vector": random_vector(vector_length),
            "limit": 100
        }

    let res_search = http.post(points_search_url, JSON.stringify(filter_payload), params);
    check(res_search, {
        'search_points is status 200': (r) => r.status === 200,
    });
}