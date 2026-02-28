import config;
import common;
import vector_storage;

[[vk::binding(0, 0)]] RWStructuredBuffer<float> scores;

[shader("compute")]
[numthreads(WAVE_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    uint idx = waveId();
    set_target(0);
    float score = similarity(idx);
    if (WaveIsFirstLane()) {
        scores[idx] = score;
    }
}
