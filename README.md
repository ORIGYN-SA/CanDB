# CanDB
**Note:** This library and repository is currently private in pre-release development. Any permission to use this library must be explictly given by the author of this library until a LICENSE is attached.

This repository holds the code for the CanDB project. CanDB is a flexible, performant, and horizontally scalable non-relational multi-canister database built for the Internet Computer.

<br/>

## Setup and Installation

CanDB is currently private, but upon being made public a user will need
to use the vessel package manager to install the CanDB module

<br/>

## Usage

To learn how set up a single canister example with CanDB see the `examples/singleCanister` folder 
* `examples/singleCanister/simpleDB` is a basic example how one might setup and integrate various canister APIs with CanDB.
* `examples/singleCanister/transactionsApp` goes into building a transactions API. This is a more complicated example that takes one through defining user access patterns and then designing your primary and sort keys to meet those requirements.

<br/>

## API Documentation

API documentation for this library can be found at https://candb.canscale.dev 

<br/>

## About CanDB 

**Entity** - An entity is the base data record or item that is stored in CanDB. It consists of:
  - Primary Key (PK) - A text/string primary key identifier used to partition your data. 
  - Sort Key (SK) - A text/string key identifier used to sort your data. Some examples might be a timestamp, an incrementing identifier, or a numerical value (turned into a string). 
  - Attributes - Additional key/value data pertaining to the entity. All attribute keys are of type text/string, and attribute values are expressed as variants, allowing for the dynamic insertion of different types of attribute values. **CanDB currently only supports Text, Int, and Bool attribute values, but can easily be expanded to support more data types**. 
  
  - The combination of an entity's primary key + sort key is unique in CanDB, meaning only one entity can have the exact same primary key and sort key.

<br/>

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


### Milestone 1 [] (In progress)
- [x] Define and implement mode of inter-canister communication between the CanDB canister manager and its storage partitions (async blocking, fire-and-forget, etc.)
- [] Design and implement cycle management and apportioning between the CanDB canister manager and storage partition canisters. Provide API allowing the developer to query remaining cycles at the canister manager level.
- [x] Implement library functionality for the CanDB Index Canister, which controls multiple storage partitions, partition storage limits, and partition data splitting logic.
- [x] Auto-Scale design and initial implementation
- [] Build and demo a simple application interacting directly with a CanDB instance canister manager which is controlling and has data stored in multiple storage partition canisters.

### Milestone 2 []
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


## License

\#TODO: Look into appropriate software license
