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
    let mut local_accumulator = Acc1::default();
    let mut global_accumulator = Acc2::default();
    let mut prev_key = None;
    for request in requests {
        let request_key = get_key(&request);
        if let Some(ref pk) = prev_key {
            if request_key != pk {
                accumulate_global(pk.clone(), local_accumulator, &mut global_accumulator)?;
                prev_key = Some(request_key.clone());
                local_accumulator = Acc1::default();
            }
        } else {
            prev_key = Some(request_key.clone());
        }
        accumulate_local(request, &mut local_accumulator)?;
    }
    if let Some(prev_key) = prev_key {
        accumulate_global(prev_key, local_accumulator, &mut global_accumulator)?;
    }
    Ok(global_accumulator)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_batch_requests(requests: &[(char, usize)]) -> Vec<(char, Vec<(char, usize)>)> {
        batch_requests::<(char, usize), char, Vec<(char, usize)>, Vec<(char, Vec<(char, usize)>)>>(
            requests.iter().copied(),
            |req| &req.0,
            |req, acc1| {
                acc1.push(req);
                Ok(())
            },
            |key, acc1, acc2| {
                acc2.push((key, acc1));
                Ok(())
            },
        )
        .unwrap()
    }

    #[test]
    fn test_batch_requests() {
        assert_eq!(
            run_batch_requests(&[('a', 1), ('b', 2), ('c', 3)]),
            vec![
                ('a', vec![('a', 1)]),
                ('b', vec![('b', 2)]),
                ('c', vec![('c', 3)]),
            ]
        );

        assert_eq!(
            run_batch_requests(&[('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5), ('c', 6)]),
            vec![
                ('a', vec![('a', 1), ('a', 2)]),
                ('b', vec![('b', 3), ('b', 4)]),
                ('c', vec![('c', 5), ('c', 6)]),
            ]
        );

        assert_eq!(
            run_batch_requests(&[('a', 1), ('b', 2), ('a', 3)]),
            vec![
                ('a', vec![('a', 1)]),
                ('b', vec![('b', 2)]),
                ('a', vec![('a', 3)]),
            ]
        );

        assert!(run_batch_requests(&[]).is_empty());
    }
}
