import config;
import common;

struct LinksParamsStruct {
    uint m;
};

[[vk::binding(0, LINKS_LAYOUT_SET)]] ConstantBuffer<LinksParamsStruct> links_params;
[[vk::binding(1, LINKS_LAYOUT_SET)]] RWStructuredBuffer<POINT_ID> links_data;

uint linksContainerOffset(uint id) { return id * (LINKS_CAPACITY + 1); }
uint linksCountIndex(uint id) { return linksContainerOffset(id); }
uint linkIndex(uint id, uint i) { return linksContainerOffset(id) + 1 + i; }
