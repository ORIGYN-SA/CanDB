import { IndexClient } from "candb-client-typescript/dist/IndexClient";
import { ActorClient } from "candb-client-typescript/dist/ActorClient";

import { idlFactory as IndexCanisterIDL } from "./declarations/index/index.did";
import { idlFactory as TestServiceCanisterIDL } from "./declarations/testService/testService.did";
import { IndexCanister } from "./declarations/index/index.did.d";
import { TestService } from "./declarations/testService/testService.did.d";

const host = "http://127.0.0.1:8000/";

// When running this locally make sure you match the hardcoded canister below with yours
export function initializeIndexClient(): IndexClient<IndexCanister> {
  return new IndexClient<IndexCanister>({
    IDL: IndexCanisterIDL,
    canisterId: "bkyz2-fmaaa-aaaaa-qaaaq-cai",
    agentOptions: {
      host,
      
    },
  });
};

export function initializeTestServiceClient(indexClient: IndexClient<IndexCanister>): ActorClient<IndexCanister, TestService> {
  return new ActorClient<IndexCanister, TestService>({
    actorOptions: {
      IDL: TestServiceCanisterIDL,
      agentOptions: {
        host,
      }
    },
    indexClient,
  })
}