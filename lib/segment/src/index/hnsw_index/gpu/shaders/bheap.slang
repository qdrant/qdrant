import config;
import common;
import shared_buffer;

struct BHeap
{
    uint offset;
    uint size;
    uint capacity;

    __init(uint offset, uint capacity)
    {
        this.offset = offset;
        this.size = 0;
        this.capacity = capacity;
    }

    ScoredPoint top() { return shared_buffer[offset]; }

    // --- Linear implementation helpers ---

    uint _findMax()
    {
        ScoredPoint max_scored_point = {0, negative_infinity};
        uint max_index = 0;
        for (uint i = WaveGetLaneIndex(); i < size; i += WAVE_SIZE) {
            bool is_large = shared_buffer[offset + i].score > max_scored_point.score;
            if (is_large) {
                max_scored_point = shared_buffer[offset + i];
                max_index = i;
            }
        }

        float subgroup_largest = WaveActiveMax(max_scored_point.score);
        bool is_max_thread = max_scored_point.score == subgroup_largest;
        uint4 subgroup_max_threads = WaveActiveBallot(is_max_thread);
        uint subgroup_max_thread = ballotFindLSB(subgroup_max_threads);
        return WaveReadLaneAt(max_index, subgroup_max_thread);
    }

    uint _findMin()
    {
        ScoredPoint min_scored_point = {0, positive_infinity};
        uint min_index = 0;
        for (uint i = WaveGetLaneIndex(); i < size; i += WAVE_SIZE) {
            bool is_less = shared_buffer[offset + i].score < min_scored_point.score;
            if (is_less) {
                min_scored_point = shared_buffer[offset + i];
                min_index = i;
            }
        }

        float subgroup_smallest = WaveActiveMin(min_scored_point.score);
        bool is_min_thread = min_scored_point.score == subgroup_smallest;
        uint4 subgroup_min_threads = WaveActiveBallot(is_min_thread);
        uint subgroup_min_thread = ballotFindLSB(subgroup_min_threads);
        return WaveReadLaneAt(min_index, subgroup_min_thread);
    }

    [mutating] bool _pushLinear(ScoredPoint score)
    {
        if (size == capacity) {
            if (score.score > shared_buffer[offset].score) {
                shared_buffer[offset] = score;
                uint min_index = _findMin();
                if (WaveIsFirstLane()) {
                    shared_buffer[offset] = shared_buffer[offset + min_index];
                    shared_buffer[offset + min_index] = score;
                }
                return true;
            } else {
                return false;
            }
        } else {
            uint i = size;
            size++;
            if (WaveIsFirstLane()) {
                if (score.score < shared_buffer[offset].score) {
                    shared_buffer[offset + i] = shared_buffer[offset];
                    shared_buffer[offset] = score;
                } else {
                    shared_buffer[offset + i] = score;
                }
            }
            return true;
        }
    }

    [mutating] ScoredPoint _popMaxLinear()
    {
        if (size == 0) {
            return {0, 0.0};
        }

        uint max_index = _findMax();
        ScoredPoint popped_value = shared_buffer[offset + max_index];

        if (WaveIsFirstLane()) {
            shared_buffer[offset + max_index] = shared_buffer[offset + size - 1];
        }

        size--;
        return popped_value;
    }

    [mutating] uint _intoArrayLinear()
    {
        sort(offset, size);
        uint result = size;
        size = 0;
        return result;
    }

    // --- Binary heap implementation helpers ---

    void _swap(uint i, uint j)
    {
        ScoredPoint tmp = shared_buffer[offset + i];
        shared_buffer[offset + i] = shared_buffer[offset + j];
        shared_buffer[offset + j] = tmp;
    }

    bool _cmp(uint i, uint j)
    {
        return shared_buffer[offset + i].score < shared_buffer[offset + j].score;
    }

    void _siftDown(uint heapSize, uint i)
    {
        while (2 * i + 1 < heapSize) {
            uint left = 2 * i + 1;
            uint right = 2 * i + 2;
            uint j = left;
            if (right < heapSize && _cmp(right, left)) {
                j = right;
            }
            if (_cmp(i, j)) {
                break;
            }
            _swap(i, j);
            i = j;
        }
    }

    void _siftUp(uint i)
    {
        while (i > 0 && _cmp(i, (i - 1) / 2)) {
            _swap(i, (i - 1) / 2);
            i = (i - 1) / 2;
        }
    }

    void _changeTop(ScoredPoint new_value)
    {
        if (WaveIsFirstLane()) {
            shared_buffer[offset] = new_value;
            _siftDown(size, 0);
        }
    }

    [mutating] bool _pushBinary(ScoredPoint score)
    {
        if (size == capacity) {
            if (score.score > shared_buffer[offset].score) {
                _changeTop(score);
                return true;
            } else {
                return false;
            }
        } else {
            uint i = size;
            size++;
            if (WaveIsFirstLane()) {
                shared_buffer[offset + i] = score;
                _siftUp(i);
            }
            return true;
        }
    }

    [mutating] ScoredPoint _popBinary()
    {
        ScoredPoint top_value = top();
        size--;
        if (WaveIsFirstLane()) {
            shared_buffer[offset] = shared_buffer[offset + size];
            _siftDown(size, 0);
        }
        return top_value;
    }

    [mutating] ScoredPoint _popMaxBinary()
    {
        if (size == 0) {
            return {0, 0.0};
        }

        ScoredPoint max_scored_point = {0, negative_infinity};
        uint max_index = 0;
        uint scan_start = (size - 1) / 2;
        for (uint i = scan_start + WaveGetLaneIndex(); i < size; i += WAVE_SIZE) {
            if (shared_buffer[offset + i].score > max_scored_point.score) {
                max_scored_point = shared_buffer[offset + i];
                max_index = i;
            }
        }

        float subgroup_largest = WaveActiveMax(max_scored_point.score);
        bool is_max_thread = max_scored_point.score == subgroup_largest;
        uint4 subgroup_max_threads = WaveActiveBallot(is_max_thread);
        uint subgroup_max_thread = ballotFindLSB(subgroup_max_threads);
        max_index = WaveReadLaneAt(max_index, subgroup_max_thread);

        ScoredPoint popped_value = shared_buffer[offset + max_index];
        if (WaveIsFirstLane()) {
            shared_buffer[offset + max_index] = shared_buffer[offset + size - 1];
            _siftUp(max_index);
        }
        size--;

        return popped_value;
    }

    [mutating] uint _intoArrayBinary()
    {
        uint array_size = size;
        while (size > 0) {
            uint i = size - 1;
            ScoredPoint value = _popBinary();
            if (WaveIsFirstLane()) {
                shared_buffer[offset + i] = value;
            }
        }
        return array_size;
    }

    // --- Public dispatch methods ---

    [mutating] bool push(ScoredPoint score)
    {
        if (BHEAP_LINEAR) return _pushLinear(score);
        else return _pushBinary(score);
    }

    [mutating] ScoredPoint popMax()
    {
        if (BHEAP_LINEAR) return _popMaxLinear();
        else return _popMaxBinary();
    }

    // intoArray must be called by ALL waves when WORKGROUP_WAVES > 1,
    // because the linear path calls sort() which uses GroupMemoryBarrierWithGroupSync.
    [mutating] uint intoArray()
    {
        if (BHEAP_LINEAR) {
            // sort() requires all waves to participate in barriers
            sort(offset, size);
            uint result = size;
            if (workgroupWaveIndex() == 0) {
                size = 0;
            }
            return result;
        } else {
            // Binary heap extraction only uses wave 0 (no cross-wave barriers)
            if (workgroupWaveIndex() == 0) {
                return _intoArrayBinary();
            } else {
                return size;
            }
        }
    }
};
