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
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    SearchContext ctx = SearchContext();
    uint subgroup_index = subgroupId();
    Request request = requests[subgroup_index];

    set_target(request.id);
    float entry_score = similarity(request.entry);
    uint search_count = ctx.search(request.id, ScoredPoint(request.entry, entry_score));

    if (WaveIsFirstLane()) {
        if (search_count > 0) {
            new_entries[subgroup_index] = shared_buffer[NEAREST_HEAP_OFFSET].id;
        } else {
            new_entries[subgroup_index] = request.entry;
        }
    }

    uint count = ctx.runHeuristic();

    // sync all threads to make sure that we don't do search while updating links
    GroupMemoryBarrierWithGroupSync();

    // update request.id links
    for (uint i = WaveGetLaneIndex(); i < count; i += SUBGROUP_SIZE) {
        links_data[linkIndex(request.id, i)] = shared_buffer[NEAREST_HEAP_OFFSET + i].id;
    }
    if (WaveIsFirstLane()) {
        links_data[linksCountIndex(request.id)] = count;
    }

    AllMemoryBarrier();

    for (uint i = 0; i < count; i++) {
        uint other_id = links_data[linkIndex(request.id, i)];

        bool is_locked = false;
        if (WaveGetLaneIndex() == 0) {
            uint other_atomic;
            InterlockedExchange(atomics[other_id], 1, other_atomic);
            is_locked = other_atomic == 1;
        }
        if (WaveActiveAnyTrue(is_locked)) {
            // point is already being processed by another subgroup
            continue;
        }

        uint other_links_count = links_data[linksCountIndex(other_id)];
        if (other_links_count < links_params.m) {
            if (WaveIsFirstLane()) {
                links_data[linkIndex(other_id, other_links_count)] = request.id;
                links_data[linksCountIndex(other_id)] = other_links_count + 1;
            }
        } else {
            set_target(other_id);
            ctx.nearest.size = 0;
            for (uint j = 0; j < other_links_count; j++) {
                POINT_ID link = links_data[linkIndex(other_id, j)];
                shared_buffer[NEAREST_HEAP_OFFSET + ctx.nearest.size] = ScoredPoint(
                    link,
                    similarity(link)
                );
                ctx.nearest.size++;
            }
            shared_buffer[NEAREST_HEAP_OFFSET + ctx.nearest.size] = ScoredPoint(
                request.id,
                similarity(request.id)
            );
            ctx.nearest.size++;

            GroupMemoryBarrier();
            sort(NEAREST_HEAP_OFFSET, ctx.nearest.size);

            GroupMemoryBarrier();
            uint other_new_links_count = ctx.runHeuristic();

            GroupMemoryBarrier();
            for (uint j = WaveGetLaneIndex(); j < other_new_links_count; j += SUBGROUP_SIZE) {
                links_data[linkIndex(other_id, j)] = shared_buffer[NEAREST_HEAP_OFFSET + j].id;
            }
            if (WaveIsFirstLane()) {
                links_data[linksCountIndex(other_id)] = other_new_links_count;
            }
        }

        AllMemoryBarrier();

        if (WaveGetLaneIndex() == 0) {
            uint dummy;
            InterlockedExchange(atomics[other_id], 0, dummy);
        }
    }
}
