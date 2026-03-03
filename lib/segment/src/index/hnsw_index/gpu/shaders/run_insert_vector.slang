import config;
import common;
import shared_buffer;
import vector_storage;
import links;
import visited_flags;
import search_context;

struct Request {
    uint id;
    uint entry;
};

[[vk::binding(0, 0)]] StructuredBuffer<Request> requests;
[[vk::binding(1, 0)]] RWStructuredBuffer<uint> new_entries;
[[vk::binding(2, 0)]] RWStructuredBuffer<uint> atomics;

[shader("compute")]
[numthreads(WAVE_SIZE, WORKGROUP_WAVES, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    SearchContext ctx = SearchContext();
    uint wave_index = waveId();
    uint wave_y = workgroupWaveIndex();
    Request request = requests[wave_index];

    set_target(request.id);
    float entry_score = similarity(request.entry);
    uint search_count = ctx.search(request.id, ScoredPoint(request.entry, entry_score));

    if (wave_y == 0 && WaveIsFirstLane()) {
        if (search_count > 0) {
            new_entries[wave_index] = shared_buffer[NEAREST_HEAP_OFFSET].id;
        } else {
            new_entries[wave_index] = request.entry;
        }
    }

    uint count = ctx.runHeuristic();

    // sync all threads to make sure that we don't do search while updating links
    GroupMemoryBarrierWithGroupSync();

    // update request.id links — all waves participate
    for (uint i = wave_y * WAVE_SIZE + WaveGetLaneIndex(); i < count; i += WORKGROUP_WAVES * WAVE_SIZE) {
        links_data[linkIndex(request.id, i)] = shared_buffer[NEAREST_HEAP_OFFSET + i].id;
    }
    if (wave_y == 0 && WaveIsFirstLane()) {
        links_data[linksCountIndex(request.id)] = count;
    }

    AllMemoryBarrierWithGroupSync();

    for (uint i = 0; i < count; i++) {
        uint other_id = links_data[linkIndex(request.id, i)];

        // Atomic lock — only wave 0, lane 0 does the exchange
        bool is_locked = false;
        if (wave_y == 0) {
            if (WaveGetLaneIndex() == 0) {
                uint other_atomic;
                InterlockedExchange(atomics[other_id], 1, other_atomic);
                is_locked = other_atomic == 1;
            }
            if (WaveIsFirstLane()) {
                scratch_loop_done = WaveActiveAnyTrue(is_locked) ? 1 : 0;
            }
        }
        GroupMemoryBarrierWithGroupSync();

        if (scratch_loop_done != 0) {
            // point is already being processed by another workgroup
            continue;
        }

        uint other_links_count = links_data[linksCountIndex(other_id)];
        if (other_links_count < links_params.m) {
            if (wave_y == 0 && WaveIsFirstLane()) {
                links_data[linkIndex(other_id, other_links_count)] = request.id;
                links_data[linksCountIndex(other_id)] = other_links_count + 1;
            }
        } else {
            set_target(other_id);
            if (wave_y == 0) {
                ctx.nearest.size = 0;
            }
            GroupMemoryBarrierWithGroupSync();

            // Score all existing links of other_id + request.id in parallel
            uint total_to_score = other_links_count + 1;
            for (uint jbatch = 0; jbatch < total_to_score; jbatch += WORKGROUP_WAVES) {
                uint my_j = jbatch + wave_y;
                POINT_ID my_point = INVALID_POINT_ID;
                float my_score = negative_infinity;

                if (my_j < total_to_score) {
                    if (my_j < other_links_count) {
                        my_point = links_data[linkIndex(other_id, my_j)];
                    } else {
                        my_point = request.id;
                    }
                    my_score = similarity(my_point);
                }

                if (WaveIsFirstLane()) {
                    scratch_results[wave_y] = ScoredPoint(my_point, my_score);
                }
                GroupMemoryBarrierWithGroupSync();

                // Wave 0 writes scored points to shared_buffer
                if (wave_y == 0 && WaveIsFirstLane()) {
                    for (uint w = 0; w < WORKGROUP_WAVES && jbatch + w < total_to_score; w++) {
                        shared_buffer[NEAREST_HEAP_OFFSET + ctx.nearest.size] = scratch_results[w];
                        ctx.nearest.size++;
                    }
                }
                GroupMemoryBarrierWithGroupSync();
            }

            // Broadcast ctx.nearest.size from wave 0 to all waves for sort()
            if (wave_y == 0 && WaveIsFirstLane()) {
                scratch_valid[0] = ctx.nearest.size;
            }
            GroupMemoryBarrierWithGroupSync();
            ctx.nearest.size = scratch_valid[0];

            sort(NEAREST_HEAP_OFFSET, ctx.nearest.size);

            GroupMemoryBarrierWithGroupSync();
            uint other_new_links_count = ctx.runHeuristic();

            GroupMemoryBarrierWithGroupSync();
            for (uint j = wave_y * WAVE_SIZE + WaveGetLaneIndex(); j < other_new_links_count; j += WORKGROUP_WAVES * WAVE_SIZE) {
                links_data[linkIndex(other_id, j)] = shared_buffer[NEAREST_HEAP_OFFSET + j].id;
            }
            if (wave_y == 0 && WaveIsFirstLane()) {
                links_data[linksCountIndex(other_id)] = other_new_links_count;
            }
        }

        AllMemoryBarrierWithGroupSync();

        if (wave_y == 0 && WaveGetLaneIndex() == 0) {
            uint dummy;
            InterlockedExchange(atomics[other_id], 0, dummy);
        }
    }
}
