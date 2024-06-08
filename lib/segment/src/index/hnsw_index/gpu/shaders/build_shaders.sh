#!/bin/bash

set -ex

glslc tests/test_vector_storage.comp -o compiled/test_vector_storage.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc tests/test_links.comp -o compiled/test_links.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc tests/test_nearest_heap.comp -o compiled/test_nearest_heap.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc tests/test_candidates_heap.comp -o compiled/test_candidates_heap.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc tests/test_visited_flags.comp -o compiled/test_visited_flags.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc tests/test_hnsw_search.comp -o compiled/test_hnsw_search.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
