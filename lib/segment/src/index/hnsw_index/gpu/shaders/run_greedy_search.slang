import config;
import common;
import vector_storage;
import links;
import visited_flags;
import search_context;

struct SearchRequest {
    uint id;
    uint entry;
};

[[vk::binding(0, 0)]] StructuredBuffer<SearchRequest> search_requests;
[[vk::binding(1, 0)]] RWStructuredBuffer<uint> search_results;

[shader("compute")]
[numthreads(WAVE_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    SearchContext ctx = SearchContext();
    uint wave_index = waveId();
    SearchRequest search_request = search_requests[wave_index];

    set_target(search_request.id);
    float entry_score = similarity(search_request.entry);
    ScoredPoint result = ctx.greedySearch(search_request.id, ScoredPoint(search_request.entry, entry_score));

    if (WaveIsFirstLane()) {
        search_results[wave_index] = result.id;
    }
}
