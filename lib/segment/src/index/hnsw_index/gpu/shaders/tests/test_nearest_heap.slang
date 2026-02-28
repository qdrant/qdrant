import config;
import common;
import shared_buffer;
import bheap;

struct TestParamsStruct {
    uint input_counts;
};

[[vk::binding(0, 0)]] ConstantBuffer<TestParamsStruct> test_params;
[[vk::binding(1, 0)]] StructuredBuffer<ScoredPoint> input_points;
[[vk::binding(2, 0)]] RWStructuredBuffer<float> scores_output;
[[vk::binding(3, 0)]] RWStructuredBuffer<uint> sorted_output;

[shader("compute")]
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    uint subgroup_index = subgroupId();

    BHeap heap = BHeap(NEAREST_HEAP_OFFSET, EF);

    for (uint i = 0; i < test_params.input_counts; i++) {
        uint point_index = subgroup_index * test_params.input_counts + i;
        ScoredPoint point = input_points[point_index];
        heap.push(point);

        GroupMemoryBarrier();
        AllMemoryBarrier();

        float nearest_smaller_score_check = WaveActiveMin(heap.top().score);
        bool is_different = nearest_smaller_score_check != heap.top().score;
        if (WaveActiveAnyTrue(is_different)) {
            if (WaveIsFirstLane()) {
                scores_output[point_index] = positive_infinity;
            }
        } else if (WaveIsFirstLane()) {
            scores_output[point_index] = heap.top().score;
        }
    }

    sort(NEAREST_HEAP_OFFSET, heap.size);
    if (WaveIsFirstLane()) {
        for (uint i = 0; i < EF; i++) {
            uint point_index = subgroup_index * EF + i;
            sorted_output[point_index] = shared_buffer[NEAREST_HEAP_OFFSET + i].id;
        }
    }
}
