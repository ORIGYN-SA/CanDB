import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";
import Text "mo:base/Text";
import Buffer "mo:stable-buffer/StableBuffer";

import TestService "TestService";

import CanisterMap "../../src/CanisterMap";
import CA "../../src/CanisterActions";
import Admin "../../src/CanDBAdmin";
import Array "mo:base/Array";
import Utils "../../src/Utils";

shared ({caller = owner}) actor class IndexCanister() = this {
  stable var pkToCanisterMap = CanisterMap.init();

  /// @required API (Do not delete or change)
  ///
  /// Get all canisters for an specific PK
  ///
  /// This method is called often by the candb-client query & update methods. 
  public shared query({caller = caller}) func getCanistersByPK(pk: Text): async [Text] {
    getCanisterIdsIfExists(pk);
  };
  
  /// Helper function that creates a test service canister for a given PK
  func createTestService(pk: Text, controllers: ?[Principal]): async Text {
    Debug.print("creating new user canister with pk=" # pk);
    Cycles.add(300_000_000_000);
    let newTestService = await TestService.TestService({
      partitionKey = pk;
      scalingOptions = {
        autoScalingHook = autoScaleTestService;
        sizeLimit = #count(3);
      };
      owners = ?[owner, Principal.fromActor(this)];
    });
    let newTestServicePrincipal = Principal.fromActor(newTestService);
    await CA.updateCanisterSettings({
      canisterId = newTestServicePrincipal;
      settings = {
        controllers = controllers;
        compute_allocation = ?0;
        memory_allocation = ?0;
        freezing_threshold = ?2592000;
      }
    });

    let newTestServiceId = Principal.toText(newTestServicePrincipal);
    pkToCanisterMap := CanisterMap.add(pkToCanisterMap, pk, newTestServiceId);

    newTestServiceId;
  };

  /// This hook is called by CanDB for AutoScaling the User Service Actor.
  ///
  /// If the developer does not spin up an additional User canister in the same partition within this method, auto-scaling will NOT work
  public shared ({caller = caller}) func autoScaleTestService(pk: Text): async Text {
    // Auto-Scaling Authorization - ensure the request to auto-scale the partition is coming from an existing canister in the partition, otherwise reject it
    if (Utils.callingCanisterOwnsPK(caller, pkToCanisterMap, pk)) {
      await createTestService(pk, ?[owner, Principal.fromActor(this)]);
    } else {
      Debug.trap("error, called by non-controller=" # debug_show(caller));
    };
  };
  
  /// Public API endpoint for spinning up a canister from the TestService actor 
  public shared({caller = creator}) func createTestServicePartition(pk: Text): async ?Text {
    let canisterIds = getCanisterIdsIfExists(pk);
    // does not exist
    if (canisterIds == []) {
      ?(await createTestService(pk, ?[owner, Principal.fromActor(this)]));
    // already exists
    } else {
      Debug.print("already exists, not creating and returning null");
      null 
    };
  };

  /// @required function (Do not delete or change)
  ///
  /// Helper method acting as an interface for returning an empty array if no canisters
  /// exist for the given PK
  func getCanisterIdsIfExists(pk: Text): [Text] {
    switch(CanisterMap.get(pkToCanisterMap, pk)) {
      case null { [] };
      case (?canisterIdsBuffer) { Buffer.toArray(canisterIdsBuffer) } 
    }
  };
}