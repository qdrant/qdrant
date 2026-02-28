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
[numthreads(WAVE_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    SearchContext ctx = SearchContext();
    uint wave_index = waveId();
    SearchRequest search_request = search_requests[wave_index];

    set_target(search_request.id);
    float entry_score = similarity(search_request.entry);
    ctx.search(search_request.id, ScoredPoint(search_request.entry, entry_score));
    uint count = ctx.runHeuristic();

    GroupMemoryBarrierWithGroupSync();

    if (WaveIsFirstLane()) {
        uint result_index_offset = EF * wave_index;
        for (uint i = 0; i < count; i++) {
            search_results[result_index_offset + i] = shared_buffer[NEAREST_HEAP_OFFSET + i];
        }
    }
}
