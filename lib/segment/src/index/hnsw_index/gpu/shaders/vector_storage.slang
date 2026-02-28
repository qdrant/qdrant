import config;
import common;
import vector_storage_dense;

struct MultivectorOffset {
    uint offset;
    uint count;
};

[[vk::binding(MULTIVECTOR_OFFSETS_BINDING, VECTOR_STORAGE_LAYOUT_SET)]] StructuredBuffer<MultivectorOffset> multivector_offsets;

static uint target_id;

void set_target(uint point_id) {
    if (MULTIVECTORS) {
        target_id = point_id;
    } else {
        set_target_dense(point_id);
    }
}

float similarity(uint point_id) {
    if (MULTIVECTORS) {
        MultivectorOffset offset_a = multivector_offsets[target_id];
        MultivectorOffset offset_b = multivector_offsets[point_id];
        float sum = 0.0;
        for (uint a = 0; a < offset_a.count; a++) {
            set_target_dense(offset_a.offset + a);
            float max_sim = negative_infinity;
            for (uint b = 0; b < offset_b.count; b++) {
                float sim = similarity_dense(offset_b.offset + b);
                if (sim > max_sim) {
                    max_sim = sim;
                }
            }
            sum += max_sim;
        }
        return sum;
    } else {
        return similarity_dense(point_id);
    }
}
