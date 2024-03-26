glslc test_vector_storage.comp -o test_vector_storage.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_links.comp -o test_links.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_searcher.comp -o test_searcher.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_greedy_searcher.comp -o test_greedy_searcher.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_heuristic.comp -o test_heuristic.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc run_requests.comp -o run_requests.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc update_entries.comp -o update_entries.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc builder_profile_helper.comp -o builder_profile_helper.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
