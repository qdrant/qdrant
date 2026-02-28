import config;
import common;
import vector_storage;

[[vk::binding(0, 0)]] RWStructuredBuffer<float> scores;

[shader("compute")]
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    uint idx = subgroupId();
    set_target(0);
    float score = similarity(idx);
    if (WaveIsFirstLane()) {
        scores[idx] = score;
    }
}
