![Horizontal_logo](https://user-images.githubusercontent.com/17368530/196894029-c2e9fefa-8ffb-47cb-8fb6-4dbb351c0529.png)


# CanDB
This repository holds the code for the CanDB project. CanDB is a flexible, performant, and horizontally scalable non-relational multi-canister database built for the Internet Computer.

<br/>

**Disclaimer:** This library and repository is currently in alpha development. This means the software has not been thoroughly tested, and that the maintainer(s) are **not** guaranteeing backwards compatibility between alpha releases at this time. This software will continue to be released as "alpha" until its code and APIs are considered stable.

<br/>

## Setup and Installation

CanDB can be installed as a Motoko module for your project using the vessel package manager.

Once vessel is installed, you can start a new project using the [candb-quickstart-template](https://github.com/canscale/candb-quickstart-template) template and CLI tool, or you can work off of one of the cloned examples in this repository located in the `examples` folder, ensuring that you pull in all of the necessary dependencies shown there for your [package-set.dhall](https://github.com/canscale/CanDB/blob/main/examples/multiCanister/simpleMultiCanister/package-set.dhall) and [vessel.dhall](https://github.com/canscale/CanDB/blob/main/examples/multiCanister/simpleMultiCanister/vessel.dhall).

<br/>

## Usage & Examples

To learn how set up a single canister example with SingleCanisterCanDB see the `examples/singleCanister` folder 
* `examples/singleCanister/simpleDB` is a basic example how one might setup and integrate various canister APIs with CanDB.
* `examples/singleCanister/transactionsApp` goes into building a transactions API. This is a more complicated example that takes one through defining user access patterns and then designing your partition and sort keys to meet those requirements.

<br/>

To learn how to set up a multi canister example with CanDB see the `examples/multiCanister` folder
* `examples/multiCanister/simpleMultiCanister` has a basic example of how one can set up an User Actor canister with CanDB, and set up an Index Canister that provides rolling upgrades, creation of new User Canisters via the PK, and auto-scaling of existing User Canisters

<br/>

## API Documentation

API documentation for this library can be found at https://candb.canscale.dev 

<br/>

## About CanDB 

**Entity** - An entity is the base data record or item that is stored in CanDB. It consists of:
  - Partition Key (PK) - A text/string partition key identifier used to partition your data. 
  - Sort Key (SK) - A text/string key identifier used to sort your data. Some examples might be a timestamp, an incrementing identifier, or a numerical value (turned into a string). 
  - Attributes - Additional key/value data pertaining to the entity. All attribute keys are of type text/string, and attribute values are expressed as variants, allowing for the dynamic insertion of different types of attribute values.
  
  - The combination of an entity's partition key + sort key is unique in CanDB, meaning only one entity can have the exact same partition key and sort key.

<br/>

## Milestones 

See MILESTONES.md to view milestone status (in progress vs. completed features)

## License
CanDB is distributed under the terms of the Apache License (Version 2.0).

See LICENSE for details.
