import config;
import common;
import shared_buffer;
import bheap;
import vector_storage;
import links;
import visited_flags;

struct SearchContext
{
    BHeap nearest;
    BHeap candidates;

    __init()
    {
        nearest = BHeap(NEAREST_HEAP_OFFSET, EF);
        candidates = BHeap(CANDIDATES_HEAP_OFFSET, EF);
    }

    // Push a scored point to the nearest and candidates heaps.
    // Must only be called by wave 0 (heap ops use wave intrinsics).
    [mutating] void processCandidate(ScoredPoint scored_point)
    {
        bool not_presented = nearest.top().id != scored_point.id;
        bool was_added = nearest.push(scored_point);
        if (was_added && not_presented) {
            candidates.push(scored_point);
            GroupMemoryBarrier();
        }
    }

    // Process links of a candidate: score each link and push to heaps.
    // All WORKGROUP_WAVES waves participate in parallel scoring.
    [mutating] void processCandidate(POINT_ID candidate_id)
    {
        uint links_count = links_data[linksCountIndex(candidate_id)];
        uint wave_y = workgroupWaveIndex();

        for (uint batch = 0; batch < links_count; batch += WORKGROUP_WAVES) {
            uint my_link_idx = batch + wave_y;
            uint my_link = INVALID_POINT_ID;
            bool my_valid = false;
            float my_score = 0;

            if (my_link_idx < links_count) {
                my_link = links_data[linkIndex(candidate_id, my_link_idx)];
                bool my_is_visited = check_visited(my_link);
                if (!my_is_visited) {
                    my_score = similarity(my_link);
                    my_valid = true;
                }
            }

            // Write results to scratch for wave 0 to consume
            if (WaveIsFirstLane()) {
                scratch_results[wave_y] = ScoredPoint(my_link, my_score);
                scratch_valid[wave_y] = my_valid ? 1 : 0;
            }
            GroupMemoryBarrierWithGroupSync();

            // Wave 0 pushes valid results to heap
            if (wave_y == 0) {
                for (uint w = 0; w < WORKGROUP_WAVES && batch + w < links_count; w++) {
                    if (scratch_valid[w] != 0) {
                        processCandidate(scratch_results[w]);
                    }
                }
            }
            GroupMemoryBarrierWithGroupSync();
        }

        // flush all changed visited flags
        DeviceMemoryBarrier();
    }

    ScoredPoint greedySearch(POINT_ID point_id, ScoredPoint entry)
    {
        ScoredPoint result = entry;
        uint wave_y = workgroupWaveIndex();

        while (true) {
            uint current = result.id;
            uint links_count = links_data[linksCountIndex(current)];
            bool found_better = false;

            for (uint batch = 0; batch < links_count; batch += WORKGROUP_WAVES) {
                uint my_link_idx = batch + wave_y;
                float my_score = negative_infinity;
                uint my_link = INVALID_POINT_ID;

                if (my_link_idx < links_count) {
                    my_link = links_data[linkIndex(current, my_link_idx)];
                    my_score = similarity(my_link);
                }

                if (WaveIsFirstLane()) {
                    scratch_results[wave_y] = ScoredPoint(my_link, my_score);
                }
                GroupMemoryBarrierWithGroupSync();

                // Wave 0 reduces results to find the best
                if (wave_y == 0) {
                    for (uint w = 0; w < WORKGROUP_WAVES && batch + w < links_count; w++) {
                        if (scratch_results[w].score > result.score) {
                            found_better = true;
                            result = scratch_results[w];
                        }
                    }
                }
                GroupMemoryBarrierWithGroupSync();
            }

            // Broadcast result from wave 0 to all waves
            if (wave_y == 0 && WaveIsFirstLane()) {
                scratch_candidate = result;
                scratch_loop_done = found_better ? 0 : 1;
            }
            GroupMemoryBarrierWithGroupSync();

            result = scratch_candidate;
            if (scratch_loop_done != 0) break;
        }
        return result;
    }

    [mutating] uint search(POINT_ID point_id, ScoredPoint entry)
    {
        uint wave_y = workgroupWaveIndex();

        if (!EXACT) {
            entry = greedySearch(point_id, entry);
        }

        // Wave 0 initializes heaps
        if (wave_y == 0) {
            if (WaveIsFirstLane()) {
                check_visited(point_id);
                check_visited(entry.id);
            }
            nearest.push(entry);
            candidates.push(entry);
            GroupMemoryBarrier();
        }
        GroupMemoryBarrierWithGroupSync();

        while (true) {
            // Wave 0 manages candidates heap
            if (wave_y == 0) {
                if (candidates.size > 0) {
                    ScoredPoint candidate = candidates.popMax();
                    if (WaveIsFirstLane()) {
                        scratch_candidate = candidate;
                        scratch_should_process = (candidate.score >= nearest.top().score) ? 1 : 0;
                        scratch_loop_done = 0;
                    }
                } else {
                    if (WaveIsFirstLane()) {
                        scratch_loop_done = 1;
                    }
                }
            }
            GroupMemoryBarrierWithGroupSync();

            if (scratch_loop_done != 0) break;
            if (scratch_should_process != 0) {
                processCandidate(scratch_candidate.id);
            }
            GroupMemoryBarrierWithGroupSync();
        }

        processCandidate(point_id);

        // Broadcast nearest.size from wave 0 to all waves before intoArray.
        // Heap push/pop only runs on wave 0, so other waves don't track size.
        // sort() inside intoArray has barriers that require all waves to agree on count.
        if (wave_y == 0 && WaveIsFirstLane()) {
            scratch_valid[0] = nearest.size;
        }
        GroupMemoryBarrierWithGroupSync();
        nearest.size = scratch_valid[0];

        // All waves must call intoArray (linear mode calls sort() with barriers)
        nearest.size = nearest.intoArray();
        GroupMemoryBarrierWithGroupSync();
        return nearest.size;
    }

    [mutating] uint runHeuristic()
    {
        uint result_count = 0;
        uint wave_y = workgroupWaveIndex();

        for (uint i = 0; i < nearest.size; i++) {
            if (result_count >= links_params.m) {
                break;
            }

            ScoredPoint current_closest = shared_buffer[NEAREST_HEAP_OFFSET + i];
            set_target(current_closest.id);

            // Reset scratch_is_good for this iteration
            if (wave_y == 0 && WaveIsFirstLane()) {
                scratch_is_good = 1;
            }
            GroupMemoryBarrierWithGroupSync();

            bool is_good = true;

            for (uint jbatch = 0; jbatch < result_count; jbatch += WORKGROUP_WAVES) {
                uint my_j = jbatch + wave_y;
                float my_dist = negative_infinity;

                if (my_j < result_count) {
                    ScoredPoint selected_point = shared_buffer[NEAREST_HEAP_OFFSET + my_j];
                    my_dist = similarity(selected_point.id);
                }

                if (WaveIsFirstLane()) {
                    scratch_dists[wave_y] = my_dist;
                }
                GroupMemoryBarrierWithGroupSync();

                // Wave 0 checks if any distance exceeds threshold
                if (wave_y == 0 && WaveIsFirstLane()) {
                    for (uint w = 0; w < WORKGROUP_WAVES && jbatch + w < result_count; w++) {
                        if (scratch_dists[w] > current_closest.score) {
                            scratch_is_good = 0;
                            break;
                        }
                    }
                }
                GroupMemoryBarrierWithGroupSync();

                if (scratch_is_good == 0) {
                    is_good = false;
                    break;
                }
            }

            if (is_good) {
                if (wave_y == 0 && WaveIsFirstLane()) {
                    shared_buffer[NEAREST_HEAP_OFFSET + result_count] = current_closest;
                }
                GroupMemoryBarrierWithGroupSync();
                result_count += 1;
            }
        }

        nearest.size = result_count;
        return result_count;
    }
};
