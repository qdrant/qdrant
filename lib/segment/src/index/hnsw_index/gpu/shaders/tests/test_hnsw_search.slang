import config;
import common;
import shared_buffer;
import vector_storage;
import links;
import visited_flags;
import search_context;

struct SearchRequest {
    uint id;
    uint entry;
};

[[vk::binding(0, 0)]] StructuredBuffer<SearchRequest> search_requests;
[[vk::binding(1, 0)]] RWStructuredBuffer<ScoredPoint> search_results;

[shader("compute")]
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    SearchContext ctx = SearchContext();
    uint subgroup_index = subgroupId();
    SearchRequest search_request = search_requests[subgroup_index];

    set_target(search_request.id);
    float entry_score = similarity(search_request.entry);
    uint count = ctx.search(search_request.id, ScoredPoint(search_request.entry, entry_score));

    GroupMemoryBarrierWithGroupSync();

    if (WaveIsFirstLane()) {
        uint result_index_offset = EF * subgroup_index;
        uint i = 0;
        for (; i < count; i++) {
            search_results[result_index_offset + i] = shared_buffer[NEAREST_HEAP_OFFSET + i];
        }
        for (; i < EF; i++) {
            search_results[result_index_offset + i] = ScoredPoint(UINT_MAX_VALUE, 0.0);
        }
    }
}
