#!/bin/bash

set -ex

glslc test_vector_storage.comp -o test_vector_storage.spv
glslc test_links.comp -o test_links.spv
glslc test_searcher.comp -o test_searcher.spv
glslc test_greedy_searcher.comp -o test_greedy_searcher.spv
glslc test_heuristic.comp -o test_heuristic.spv
glslc apply_responses.comp -o apply_responses.spv
glslc run_requests.comp -o run_requests.spv
glslc update_entries.comp -o update_entries.spv
