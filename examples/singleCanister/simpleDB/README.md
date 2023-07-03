# SimpleDB Application Example (Using CanDB)

## Setup instructions (for local deployment and testing)

**Note:** Since CanDB is not yet public, In order to run this working example, the commands below 
need to be run from the examples/simpleDB directory

1. Ensure you are using dfx version `0.14.1` (specified in the `dfx.json`)
2. Create the simpleDB canister on the local network -> run `dfx canister create simpleDB`
3. Ensure the simpleDB application builds -> run `dfx build simpleDB`
4. Deploy the canister to the local network -> run `dfx deploy simpleDB`
5. Test out the simpleDB application through the Candid UI link returned in step 4),
or by building your own client (frontend, other canister, etc.) to interact with it.
