What guarantees should be provided:
 
- Once data is submitter successfully, it should not be lost (assuming power loss didn't happened in given persistence time interval).
- While indexing, power loss may appear in any moment. Index should not be corrupted.
- All submitted data should eventually be available.

```mermaid
graph TB
	request --> WAL
	WAL --> indexer
	subgraph collections
		register
		segments
	end
	searchers>searchers] -- read --> register
	indexer>indexer] -- write --> register
	register -- owns --> segments
```

## Update stages
### Initial structure
```mermaid
graph LR
	collection -- names -->register
	subgraph collection1
		subgraph segment1
			meta1 --> raw_data1
			meta1 --> index1
			index1 -.- raw_data1
		end
		subgraph segment2
			meta2 --> raw_data2
			meta2 --> index2
			index2 -.- raw_data2
		end
		register --> meta1
		register --> meta2
	end
```

### Update

What could be updated? 

* Alter labels (Update or delete)
	* It involves index update + statistics update
* Insert new point
* Delete point
* Rename collection
* Optimize \ merge segments
* Collection dropped
* Collection created
