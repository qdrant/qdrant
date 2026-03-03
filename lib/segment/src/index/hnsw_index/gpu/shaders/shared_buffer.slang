import config;
import common;

groupshared ScoredPoint shared_buffer[SHARED_BUFFER_CAPACITY];

// Scratch area for multi-wave inter-wave communication.
groupshared ScoredPoint scratch_results[WORKGROUP_WAVES];
groupshared uint scratch_valid[WORKGROUP_WAVES];
groupshared float scratch_dists[WORKGROUP_WAVES];
groupshared ScoredPoint scratch_candidate;
groupshared uint scratch_should_process;
groupshared uint scratch_loop_done;
groupshared uint scratch_is_good;

uint _subgroup_bubble_sort(float score) {
    uint invocation = WaveGetLaneIndex();
    bool is_even = invocation % 2 == 0;
    uint invocation_forward = is_even ? invocation + 1 : invocation - 1;
    uint invocation_backward = invocation;
    if (invocation != WAVE_SIZE - 1 && invocation != 0) {
        invocation_backward = is_even ? invocation - 1 : invocation + 1;
    }

    uint index = invocation;
    for (uint j = 0; j < WAVE_SIZE / 2; j++) {
        bool is_even = WaveGetLaneIndex() % 2 == 0;
        bool was_changed = false;

        // step one: compare with the next element
        {
            float next_score = WaveReadLaneAt(score, invocation_forward);
            uint next_index = WaveReadLaneAt(index, invocation_forward);
            bool need_exchange = is_even ? score < next_score : score > next_score;
            if (need_exchange) {
                was_changed = true;
                score = next_score;
                index = next_index;
            }
        }

        // step two: compare with previous element
        {
            float next_score = WaveReadLaneAt(score, invocation_backward);
            uint next_index = WaveReadLaneAt(index, invocation_backward);
            bool need_exchange = is_even ? score > next_score : score < next_score;
            if (need_exchange) {
                was_changed = true;
                score = next_score;
                index = next_index;
            }
        }

        if (!WaveActiveAnyTrue(was_changed)) {
            break;
        }
    }
    return index;
}

void _bitonic_cmp(uint offset, uint count, uint b_i, uint b_j) {
    if (b_i < b_j && b_j < count && shared_buffer[offset + b_j].score > shared_buffer[offset + b_i].score) {
        ScoredPoint temp = shared_buffer[offset + b_i];
        shared_buffer[offset + b_i] = shared_buffer[offset + b_j];
        shared_buffer[offset + b_j] = temp;
    }
}

// Sort the shared buffer in place using bitonic sort
void sort(uint offset, uint count) {
    uint wave_y = workgroupWaveIndex();

    // To increase performance, sort blocks of size `WAVE_SIZE` using bubble sort.
    // Each wave sorts its own block in parallel.
    for (uint group_offset = wave_y * WAVE_SIZE; group_offset < count; group_offset += WORKGROUP_WAVES * WAVE_SIZE) {
        ScoredPoint scored_point = {0, negative_infinity};
        uint i = group_offset + WaveGetLaneIndex();
        if (i < count) {
            scored_point = shared_buffer[offset + i];
        }

        uint permut = _subgroup_bubble_sort(scored_point.score);
        ScoredPoint shuffled = {
            WaveReadLaneAt(scored_point.id, permut),
            WaveReadLaneAt(scored_point.score, permut)
        };
        scored_point = shuffled;

        if (i < count) {
            shared_buffer[offset + i] = scored_point;
        }
    }
    GroupMemoryBarrierWithGroupSync();

    // Start bitonic sort. Start from `k = 2 * WAVE_SIZE` instead of `1` because we have already sorted blocks of size `WAVE_SIZE`.
    // All waves participate in the bitonic merge phase.
    for (uint k = 2 * WAVE_SIZE; (k >> 1) < count; k <<= 1) {
        for (uint i = wave_y * WAVE_SIZE + WaveGetLaneIndex(); i < count; i += WORKGROUP_WAVES * WAVE_SIZE) {
            _bitonic_cmp(offset, count, i, i ^ (k - 1));
        }
        GroupMemoryBarrierWithGroupSync();

        for (uint j = k >> 1; 0 < j; j >>= 1) {
            for (uint i = wave_y * WAVE_SIZE + WaveGetLaneIndex(); i < count; i += WORKGROUP_WAVES * WAVE_SIZE) {
                _bitonic_cmp(offset, count, i, i ^ j);
            }
            GroupMemoryBarrierWithGroupSync();
        }
    }
}
