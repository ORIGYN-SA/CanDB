import { initializeIndexClient, initializeTestServiceClient } from "./client";
import { IndexClient } from "candb-client-typescript/dist/IndexClient";
import { ActorClient } from "candb-client-typescript/dist/ActorClient";
import { IndexCanister } from "./declarations/index/index.did.d";
import { TestService } from "./declarations/testService/testService.did.d";

let indexClient: IndexClient<IndexCanister>;
let testServiceClient: ActorClient<IndexCanister, TestService>;

const pk = "pk-multiple-single-inserts";

describe("Multiple single inserts", () => {
  beforeAll(async () => {
    indexClient = initializeIndexClient();
    testServiceClient = initializeTestServiceClient(indexClient);
    // create the partition
    await indexClient.indexCanisterActor.createTestServicePartition(pk);
    // insert 20 entities (auto-scaling should happen at 3)
    let array20 = [...Array(20)].map((_, i) => i);
    await Promise.all(array20.map((i) => {
      const sk = i.toString(); 
      return testServiceClient.update<TestService["addEntity"]>(
      pk,
      sk,
      actor => actor.addEntity(sk, BigInt(i))
    )}));
  });

  it("partition should have 2 canisters, with 20 in one and zero in the other", async () => {
    //let canistersInPK = await indexClient.getCanistersForPK("pk");
    let canisterDBSizes = await testServiceClient.query<TestService["getDBSize"]>(
      pk,
      actor => actor.getDBSize(),
    );
    // expect auto-scaling to have happened
    expect(canisterDBSizes.length).toBe(2);
    if (canisterDBSizes[0].status === "rejected" || canisterDBSizes[1].status === "rejected") throw new Error("Unreachable")

    let canisterValues = [canisterDBSizes[0].value, canisterDBSizes[1].value];
    // expect all inserts to be in the first canister
    expect(canisterValues).toEqual([20n, 0n])
  });
});