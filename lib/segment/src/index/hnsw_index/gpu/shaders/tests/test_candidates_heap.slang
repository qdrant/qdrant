import config;
import common;
import shared_buffer;
import bheap;

struct TestParamsStruct {
    uint input_counts;
};

[[vk::binding(0, 0)]] ConstantBuffer<TestParamsStruct> test_params;
[[vk::binding(1, 0)]] StructuredBuffer<ScoredPoint> input_points;
[[vk::binding(2, 0)]] RWStructuredBuffer<ScoredPoint> scores_output;

[shader("compute")]
[numthreads(SUBGROUP_SIZE, 1, 1)]
void main(uint3 dtid : SV_DispatchThreadID) {
    gl_GlobalInvocationID = dtid;
    uint subgroup_index = subgroupId();

    BHeap heap = BHeap(CANDIDATES_HEAP_OFFSET, EF);

    for (uint i = 0; i < test_params.input_counts; i++) {
        uint point_index = subgroup_index * test_params.input_counts + i;
        ScoredPoint point = input_points[point_index];
        heap.push(point);
    }

    while (heap.size > 0) {
        ScoredPoint candidate = heap.popMax();
    }

    for (uint i = 0; i < test_params.input_counts; i++) {
        uint point_index = subgroup_index * test_params.input_counts + i;
        ScoredPoint point = input_points[point_index];
        heap.push(point);
    }

    uint output_index = subgroup_index * test_params.input_counts;
    while (heap.size > 0) {
        ScoredPoint candidate = heap.popMax();
        if (WaveIsFirstLane()) {
            scores_output[output_index] = candidate;
        }
        output_index++;
    }
}
