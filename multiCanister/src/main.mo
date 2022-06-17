//import CanDB "../../../src/CanDBv2";
import CanisterMap "./CanisterMap";
import Entity "../../../src/Entity";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Buffer "mo:stable-buffer/StableBuffer";
import Cycles "mo:base/ExperimentalCycles";
import Error "mo:base/Error";
import Array "mo:base/Array";

import UserCanister "./UserCanister";
import CA "CanisterActions";

// Canister Manager

/// Blogger User
/// 1. get a specific blog post
/// 2. get latest blog posts 
/// 3. get latest comments for a blog post
/// 4. get user settings
/// 5. create a blog post
/// 6. Create comments for a specific blog post

/// Outside user
/// 7. Follow a blogger
/// 8. get a specific blog post
/// 9. Get latest blog posts for a specific blogger
/// 10. Create comments for a specific blog post


shared ({caller = owner}) actor class CanisterManager() = this {
  stable var pkToCanisterMap = CanisterMap.init();

  public shared query({caller = caller}) func getCanistersForPK(pk: Entity.PK): async [Text] {
    switch(pk) {
      case "CALLER_LOOKUP" { getCanisterIdsIfExists("#user" # Principal.toText(caller)) };
      case _ {
        if (Text.startsWith(pk, #text("user#"))) {
          return []
        } else {
          getCanisterIdsIfExists(pk)
        }
      }
    };
  };

  public shared({caller = creator}) func createUser(): async ?Text {
    let callerPrincipalId = Principal.toText(creator);
    let userPk = "user#" # callerPrincipalId;
    let canisterIds = getCanisterIdsIfExists(userPk);
    // does not exist, create first canister for user
    if (canisterIds == []) {
      Debug.print("creating canister for pk=" # userPk);
      Cycles.add(300_000_000_000);
      let newUserCanister = await UserCanister.UserCanister(userPk);
      let newUserCanisterPrincipal = Principal.fromActor(newUserCanister);
      await CA.updateCanisterSettings({
        canisterId = newUserCanisterPrincipal;
        settings = {
          controllers = ?[owner, Principal.fromActor(this)];
          compute_allocation = ?0;
          memory_allocation = ?0;
          freezing_threshold = ?2592000;
        }
      });

      let newUserCanisterId = Principal.toText(newUserCanisterPrincipal);
      pkToCanisterMap := CanisterMap.add(pkToCanisterMap, userPk, newUserCanisterId);

      ?newUserCanisterId;
    } else {
      // already exists, don't recreate
      Debug.print("already exists, not creating and returning null");
      null 
    }
  };

  type DeletionStatus = { 
    canisterId: Text;
    status: { #deleted; #stopped; #transferedCycles; #error: Text };
  };

  public shared({caller = caller}) func deleteLoggedInUser(): async () {
    let callerPrincipalId = Principal.toText(caller);
    let userPk = "user#" # callerPrincipalId;
    let canisterIds = getCanisterIdsIfExists(userPk);
    if (canisterIds == []) {
      Debug.print("canister for user with principal=" # callerPrincipalId # " pk=" # userPk # " does not exist");
    } else {
      //let deletionStatus = Array.map<Text, DeletionStatus>(canisterIds, deleteCanister);
      let deletionStatus = await deleteCanisters(canisterIds); 
      await deleteCanisters(canisterIds); 

      /*
      let canisterPrincipal  = Principal.fromText(canisterId);
      let userActor = actor(canisterId): actor { transferCycles: () -> async() };
      await userActor.transferCycles();
      await CA.stopCanister(canisterPrincipal);
      await CA.deleteCanister(canisterPrincipal);
      Debug.print("deleted " # canisterId);
      */
    };
    pkToCanisterMap := CanisterMap.delete(pkToCanisterMap, userPk);
  };

  //func deleteCanister(canisterId: Text): DeletionStatus {
  func deleteCanisters(canisterIds: [Text]): async () { // [DeletionStatus] {
    let canisterPrincipals = Array.map<Text, Principal>(canisterIds, Principal.fromText); 
    let transferResult = await transferAllCycles(canisterIds);
    let stoppedResult = await stopAllCanisters(canisterPrincipals);
    let deletedResult = await deleteAllCanisters(canisterPrincipals);
    /*
    try {
      await canister.transferCycles();
      await CA.stopCanister(canisterPrincipal);
      await CA.deleteCanister(canisterPrincipal);
      Debug.print("deleted " # canisterId);
      await {
        canisterId = canisterId;
        status = #deleted;
      };
    } catch (err) 
      Debug.print("error deleting canisterId=" # canisterId # ", error=" # Error.message(error))
      {
        canisterId = canisterId;
        status = #error(Error.message(error));
      }
    }
    */
  };

  public func transferAllCycles(cids: [Text]): async [Text] {
    let actors = Array.map<Text, actor {transferCycles : shared () -> async Text}>(cids, func(id) { 
      actor(id): actor { transferCycles: shared () -> async Text };
    });
    let executingTransferCycles = Buffer.init<async Text>();
    for (a in actors.vals()) {
      Buffer.add<async Text>(executingTransferCycles, a.transferCycles());
    };
    let collectingTransferCycles = Buffer.init<Text>();
    var i = 0;
    label l loop {
      if (i >= executingTransferCycles.count) break l;
      Buffer.add(collectingTransferCycles, await executingTransferCycles.elems[i]);
      i += 1;
    };

    Buffer.toArray(collectingTransferCycles);
  };

  public func stopAllCanisters(canisterPrincipals: [Principal]): async [Text] {
    let executingStopCanisters = Buffer.init<async Text>();
    for (principal in canisterPrincipals.vals()) {
      Buffer.add<async Text>(executingStopCanisters, CA.stopCanister(principal));
    };
    let collectingStoppedCanisters = Buffer.init<Text>();
    var i = 0;
    label l loop {
      if (i >= executingStopCanisters.count) break l;
      Buffer.add(collectingStoppedCanisters, await executingStopCanisters.elems[i]);
      i += 1;
    };

    Buffer.toArray(collectingStoppedCanisters);
  };

  public func deleteAllCanisters(canisterPrincipals: [Principal]): async [Text] {
    let executingDeleteCanisters = Buffer.init<async Text>();
    for (principal in canisterPrincipals.vals()) {
      Buffer.add<async Text>(executingDeleteCanisters, CA.stopCanister(principal));
    };
    let collectingDeletedCanisters = Buffer.init<Text>();
    var i = 0;
    label l loop {
      if (i >= executingDeleteCanisters.count) break l;
      Buffer.add(collectingDeletedCanisters, await executingDeleteCanisters.elems[i]);
      i += 1;
    };
    Buffer.toArray(collectingDeletedCanisters);
  };




  // TODO: In progress setting up rolling upgrades to child canisters
  /*
  public shared({ caller = caller }) func upgradeUserCanisters(wasmModule: [Nat8]): async () {
    //if (caller == owner) {
    for ((pk, canisterId) in CanisterMap.entries(pkToCanisterMap)) {
      Debug.print("upgrading canister: " # canisterId);
      let blobPK = Text.encodeUtf8(pk);
      let blobOwners = [Principal.toBlob(owner), Principal.toBlob(Principal.fromActor(this))];
      try {
        await CA.upgradeCanisterCode(
          Principal.fromText(canisterId),
          wasmModule
        );
        Debug.print("finished upgrading canister: " # canisterId);
      } catch err {
        Debug.print("upgrading canister:" # canisterId # " failed");
      };
      await IC.install_code({
        mode = #upgrade;
        canister_id = Principal.fromText(canisterId);
        wasm_module = wasmModule;
      });
    };
  };
  */

  func getCanisterIdsIfExists(pk: Entity.PK): [Text] {
    switch(CanisterMap.get(pkToCanisterMap, pk)) {
      case null { [] };
      case (?canisterIdsBuffer) { Buffer.toArray(canisterIdsBuffer) } 
    }
  };
}