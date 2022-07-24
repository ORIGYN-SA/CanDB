## Milestones 

### 30-day sprint [x] (Complete!)

- [x] Implement CanDB APIs running on the canister manager such that the stored data is persisted and stable through upgrades. APIs include:
  - [x] Create (Capability shown through using CanDB.update()) in the simpleDB example
  - [x] Put/Update
  - [x] Get 
  - [x] Delete
  - [x] Scan (across a primary key + range/sort key combination)
- [x] Complete unit testing for all CanDB APIs
- [x] Implement Red-Black tree split algorithm (For halving a partition)


### Milestone 1 [x] (Complete!)
- [x] Define and implement mode of inter-canister communication between the CanDB canister manager and its storage partitions (async blocking, fire-and-forget, etc.)
- [x] Build example demonstrating the ability to apportion cycles from the Index Canister to newly created canisters
- [x] Implement library functionality for the CanDB Index Canister, which controls multiple storage partitions, partition storage limits, and partition data splitting logic.
- [x] Auto-Scale design and initial implementation
- [x] Build and demo a simple application interacting directly with a CanDB instance canister manager which is controlling and has data stored in multiple storage partition canisters.

### CanDB alpha goals (In progress, complete in tandem with Milestone 2)
- [] Cultivate an passionate alpha community dedicated to building at scale and pushing the limits of what's possible on the IC
- [] Reduce developer adoption friction by improve CanDB documentation and quickstart tooling
- [] Refine existing CanDB and CanDBAdmin APIs through developer feedback
- [] Receive cycle management feedback, leading to the appropriate API abstraction
  - [] Design and implement cycle management and apportioning between the CanDB canister manager and storage partition canisters. Provide API allowing the developer to query remaining cycles at the canister manager level.
- [] Use developer input/feedback to help scope and prioritize new features.

### Milestone 2 [] (In progress)
- [] Measure and fine tune performance of CanDB APIs as the number of records and storage partitions increase
  - [] Define metrics, such as:
    - query/update performance
    - canister memory used
    - request throughput (number of concurrent query/update calls CanDB is able to process in a short burst of time)
  - [] Test CanDB against these metrics as the number of records/partition and number of partitions increase, ensuring that these metrics match the expected runtime and performance - i.e. proof of CanScale ;)
- [] Identify and measure performance bottlenecks, and attempt to improve upon these bottlenecks if possible
  - [] Measure inter-canister query times and look into performance tradeoffs between direct canister calls and calls proxied through the canister manager to its partitions
  - [] Depending on the performance of the Red-Black Tree in ordering records, implement a BTree library for underlying CanDB data structures allowing primary key and range/sort key access, as well as splitting a storage partition and spinning up "n" numbers new storage partitions based on the order of the BTree.
- [] Perform data stability testing, ensuring that data stored in CanDB is persisted through upgrades
- [] Research and test other potential "chaos" scenarios that might affect data stability.
- [] Add integration testing