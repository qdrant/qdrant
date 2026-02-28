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

    [mutating] void processCandidate(ScoredPoint scored_point)
    {
        bool not_presented = nearest.top().id != scored_point.id;
        bool was_added = nearest.push(scored_point);
        if (was_added && not_presented) {
            candidates.push(scored_point);
            GroupMemoryBarrier();
        }
    }

    [mutating] void processCandidate(POINT_ID candidate_id)
    {
        uint links_count = links_data[linksCountIndex(candidate_id)];
        uint subgroup_link = 0;
        bool subgroup_is_visited = true;
        uint invocation = WaveGetLaneIndex();

        for (uint i = 0; i < links_count; i++) {
            if (i % SUBGROUP_SIZE == 0 && i + invocation < links_count) {
                subgroup_link = links_data[linkIndex(candidate_id, i + invocation)];
                subgroup_is_visited = check_visited(subgroup_link);
            }

            uint link = WaveReadLaneAt(subgroup_link, i % SUBGROUP_SIZE);
            bool is_visited = WaveReadLaneAt(subgroup_is_visited, i % SUBGROUP_SIZE);

            if (!is_visited) {
                float score = similarity(link);
                ScoredPoint scored_point = ScoredPoint(link, score);
                processCandidate(scored_point);
            }
        }

        // flush all changed visited flags
        DeviceMemoryBarrier();
    }

    ScoredPoint greedySearch(POINT_ID point_id, ScoredPoint entry)
    {
        bool changed = true;
        ScoredPoint result = entry;

        uint subgroup_link = 0;
        uint invocation = WaveGetLaneIndex();

        while (changed) {
            changed = false;

            uint current = result.id;
            uint links_count = links_data[linksCountIndex(current)];
            for (uint i = 0; i < links_count; i++) {
                if (i % SUBGROUP_SIZE == 0 && i + invocation < links_count) {
                    subgroup_link = links_data[linkIndex(current, i + invocation)];
                }
                uint link = WaveReadLaneAt(subgroup_link, i % SUBGROUP_SIZE);

                float score = similarity(link);
                if (score > result.score) {
                    changed = true;
                    result = ScoredPoint(link, score);
                }
            }
        }
        return result;
    }

    [mutating] uint search(POINT_ID point_id, ScoredPoint entry)
    {
        if (!EXACT) {
            entry = greedySearch(point_id, entry);
        }

        if (WaveIsFirstLane()) {
            check_visited(point_id);
            check_visited(entry.id);
        }
        nearest.push(entry);
        candidates.push(entry);
        GroupMemoryBarrier();

        while (candidates.size > 0) {
            ScoredPoint candidate = candidates.popMax();
            if (candidate.score < nearest.top().score) {
                continue;
            }
            processCandidate(candidate.id);
        }

        processCandidate(point_id);

        nearest.size = nearest.intoArray();
        return nearest.size;
    }

    [mutating] uint runHeuristic()
    {
        uint result_count = 0;

        for (uint i = 0; i < nearest.size; i++) {
            if (result_count >= links_params.m) {
                break;
            }

            ScoredPoint current_closest = shared_buffer[NEAREST_HEAP_OFFSET + i];
            set_target(current_closest.id);
            bool is_good = true;

            for (uint j = 0; j < result_count; j++) {
                ScoredPoint selected_point = shared_buffer[NEAREST_HEAP_OFFSET + j];
                float dist_to_already_selected = similarity(selected_point.id);
                if (dist_to_already_selected > current_closest.score) {
                    is_good = false;
                    break;
                }
            }

            if (is_good) {
                if (WaveIsFirstLane()) {
                    shared_buffer[NEAREST_HEAP_OFFSET + result_count] = current_closest;
                }
                GroupMemoryBarrier();
                result_count += 1;
            }
        }

        nearest.size = result_count;
        return result_count;
    }
};
