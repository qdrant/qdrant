use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;


pub type LinkContainer = Vec<PointOffsetType>;
pub type LayerContainer = Vec<LinkContainer>;



#[derive(Deserialize, Serialize, Clone, Debug)]
struct GraphLayer {
    links: LinkContainer
}


impl GraphLayer {

}


