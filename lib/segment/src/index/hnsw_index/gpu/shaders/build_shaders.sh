#!/bin/bash

set -ex

glslc test_vector_storage.comp -o test_vector_storage.spv -O
glslc test_links.comp -o test_links.spv -O
glslc test_searcher.comp -o test_searcher.spv -O
glslc test_greedy_searcher.comp -o test_greedy_searcher.spv -O
glslc test_heuristic.comp -o test_heuristic.spv -O
glslc run_requests.comp -o run_requests.spv -O
glslc update_entries.comp -o update_entries.spv -O
glslc builder_profile_helper.comp -o builder_profile_helper.spv -O
