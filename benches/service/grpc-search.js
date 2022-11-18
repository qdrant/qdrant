import grpc from 'k6/net/grpc';
import { check, group } from 'k6';
import exec from 'k6/execution';

import { random_city, random_vector } from '/code/utils.js';

// test system parameters
let host = 'localhost:6334'
let collection_name = 'grpc_stress';

// test payload parameters
let vector_length = 128;

const client = new grpc.Client();
client.load(['/proto'], 'collections_service.proto', 'points_service.proto');

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

export function search_points() {
     // connect client on first iteration
     // https://github.com/grafana/k6/issues/2719#issuecomment-1280033675
     if (exec.vu.iterationInScenario == 0) {
        client.connect(host, { plaintext: true });
     }

    // generate random search query
    var filter_payload =
        {
            collection_name: collection_name,
            vector: random_vector(vector_length),
            filter: {
                must: [
                    {
                        field: {
                            key: "city",
                            match: {
                                keyword: random_city()
                            }
                        }
                    }
                ]
            },
            with_vectors: {
              enable: true,
            },
            with_payload: {
              enable: true,
            },
            limit: 100
        }

    let res_search = client.invoke('qdrant.Points/Search', filter_payload);
    //console.log(res_search.body);
    check(res_search, {
        'search_points status is 200': (r) => r.status === grpc.StatusOK,
    });
}
