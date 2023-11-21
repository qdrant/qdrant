use crate::operations::types::CollectionResult;

/// The function performs batching processing of read requests, that some arbitrary key
///
/// Functions are split into sequential subgroups based on the shard key.
/// There are two customizable aggregation functions:
///
/// * `accumulate_local` - called for each request to form a subgroup
/// * `accumulate_global` - called for each subgroup accumulated by `accumulate_local`
///
/// The function returns the result of the last call of `accumulate_global` function.
///
///
/// Example usage (simplified):
///
/// requests = [
///     Recommend(positive=[1], shard_key="cats"),
///     Recommend(positive=[2], shard_key="cats"),
///     Recommend(positive=[3], shard_key="dogs"),
///     Recommend(positive=[3], shard_key="dogs"),
/// ]
///
/// We want to:
///     1. Group requests by shard_key into Acc1 (vector of requests)
///     2. Execute each group of requests and push the result into Acc2 (vector of results)
///
/// How to do that:
///
/// ```rust,ignore
/// batch_requests::<
///     Recommend,             // Type of request
///     String,                // Type of shard_key
///     Vec<Recommend>,        // Type of local accumulator
///     Vec<Vec<ScoredPoint>>, // Type of global accumulator,
/// >(
///     requests,
///     |request| &request.shard_key,
///     |request, local_accumulator| { // Accumulate requests
///         local_accumulator.push(request);
///         // Note: we can have more complex logic here
///         // E.g. extracting IDs from requests and de-duplicating them
///         Ok(())
///     },
///     |shard_key, local_accumulator, global_accumulator| { // Execute requests and accumulate results
///        let result = execute_recommendations(local_accumulator, shard_key);
///        global_accumulator.push(result);
///        Ok(())
///     }
/// )
/// ```
pub fn batch_requests<Req, Key: PartialEq + Clone, Acc1: Default, Acc2: Default>(
    requests: impl IntoIterator<Item = Req>,
    get_key: impl Fn(&Req) -> &Key,
    mut accumulate_local: impl FnMut(Req, &mut Acc1) -> CollectionResult<()>,
    mut accumulate_global: impl FnMut(Key, Acc1, &mut Acc2) -> CollectionResult<()>,
) -> CollectionResult<Acc2> {
    let mut requests_iterator = requests.into_iter();

    let mut request = requests_iterator.next().unwrap();
    let mut prev_key = get_key(&request).clone();

    let mut local_accumulator = Acc1::default();
    let mut global_accumulator = Acc2::default();

    loop {
        let request_key = get_key(&request);
        if &prev_key != request_key {
            accumulate_global(prev_key, local_accumulator, &mut global_accumulator)?;
            prev_key = request_key.clone();
            local_accumulator = Acc1::default();
        }

        accumulate_local(request, &mut local_accumulator)?;

        if let Some(next_request) = requests_iterator.next() {
            request = next_request;
        } else {
            break;
        }
    }
    accumulate_global(prev_key, local_accumulator, &mut global_accumulator)?;

    Ok(global_accumulator)
}
