use shard::count::CountRequestInternal;

use crate::requests::count::CountRequest;

impl From<CountRequest> for CountRequestInternal {
    fn from(request: CountRequest) -> Self {
        let CountRequest { filter, exact } = request;
        CountRequestInternal { filter, exact }
    }
}
