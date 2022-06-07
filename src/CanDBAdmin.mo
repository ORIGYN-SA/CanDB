/// CanDBAdmin - asynchronous child canister control utility methods to be called by the IndexCanister

import RBT "mo:stable-rbtree/StableRBTree";
import Buffer "mo:stable-buffer/StableBuffer";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Principal "mo:base/Principal";
import Cycles "mo:base/ExperimentalCycles";
import CA "./CanisterActions";
import CanisterMap "./CanisterMap";
import CanDB "./CanDB";

import Error "mo:base/Error";
import Iter "mo:base/Iter";

module {
  public type InterCanisterActionResult = { #ok; #err: Text }; 
  type InterCanisterStatusMap = RBT.Tree<Text, InterCanisterActionResult>;

  public type CanisterCleanupStatusMap = {
    transfer: RBT.Tree<Text, InterCanisterActionResult>;
    stop: RBT.Tree<Text, InterCanisterActionResult>;
    delete: RBT.Tree<Text, InterCanisterActionResult>;
  };

  /// Cleans up a list of canisterIds in parallel. Returns the resulting status of each of the canisterIds after cleanup 
  ///
  /// 1. Calls the TransferCycles method of each actor 
  /// 2. Stops each canistier
  /// 3. Deletes each canister
  public func transferCyclesStopAndDeleteCanisters(canisterIds: [Text]): async CanisterCleanupStatusMap {
    let canisterPrincipals = Array.map<Text, Principal>(canisterIds, func(id) {
      Principal.fromText(id);
    });
    let transferedResult = await transferAllCycles(canisterIds);
    let stoppedResult = await stopAllCanisters(canisterPrincipals);
    let deletedResult = await deleteAllCanisters(canisterPrincipals);
    {
      transfer = transferedResult;
      stop = stoppedResult;
      delete = deletedResult;
    }
  };

  /// Attempts to transfer cycles from canisters (by canisterId) to the calling canister 
  ///
  /// canisterIds - each of the canisterIds that will transfer cycles to the calling canister
  public func transferAllCycles(canisterIds: [Text]): async InterCanisterStatusMap {
    let actors = Array.map<Text, actor { transferCycles : shared () -> async () }>(canisterIds, func(id) { 
      actor(id): actor { transferCycles: shared () -> async () };
    });
    let executingTransferCycles = Buffer.init<async ()>();
    for (a in actors.vals()) {
      Buffer.add<async ()>(executingTransferCycles, a.transferCycles());
    };
    var transferStatusMap = RBT.init<Text, InterCanisterActionResult>();
    var i = 0;
    label l loop {
      if (i >= executingTransferCycles.count) break l;
      try {
        let success = await executingTransferCycles.elems[i];
        transferStatusMap := RBT.put<Text, InterCanisterActionResult>(transferStatusMap, Text.compare, canisterIds[i], #ok);
      } catch (error) {
        transferStatusMap := RBT.put<Text, InterCanisterActionResult>(transferStatusMap, Text.compare, canisterIds[i], #err(Error.message(error)));
      };
      i += 1;
    };

    Debug.print("all transfers complete"); 
    transferStatusMap;
  };

  /// Attempts to stop each of the canisters (by Principal) from the calling canister 
  ///
  /// canisterPrincipals - each of the canisterPrincipals of the canisters that will be stopped 
  public func stopAllCanisters(canisterPrincipals: [Principal]): async InterCanisterStatusMap {
    let executingStopCanisters = Buffer.init<async ()>();
    for (principal in canisterPrincipals.vals()) {
      Buffer.add<async ()>(executingStopCanisters, CA.stopCanister(principal));
    };
    var stoppedStatusMap = RBT.init<Text, InterCanisterActionResult>();
    var i = 0;
    label l loop {
      if (i >= executingStopCanisters.count) break l;
      try {
        let success = await executingStopCanisters.elems[i];
        stoppedStatusMap := RBT.put<Text, InterCanisterActionResult>(
          stoppedStatusMap,
          Text.compare,
          Principal.toText(canisterPrincipals[i]),
          #ok
        );
      } catch (error) {
        stoppedStatusMap := RBT.put<Text, InterCanisterActionResult>(
          stoppedStatusMap,
          Text.compare,
          Principal.toText(canisterPrincipals[i]),
          #err(Error.message(error))
        );
      };
      i += 1;
    };

    Debug.print("all stops complete"); 
    stoppedStatusMap;
  };

  /// Attempts to delete each of the canisters (by Principal) from the calling canister 
  ///
  /// canisterPrincipals - each of the canisterPrincipals of the canisters that will be deleted 
  public func deleteAllCanisters(canisterPrincipals: [Principal]): async InterCanisterStatusMap {
    let executingDeleteCanisters = Buffer.init<async ()>();
    for (principal in canisterPrincipals.vals()) {
      Buffer.add<async ()>(executingDeleteCanisters, CA.deleteCanister(principal));
    };
    var deletedStatusMap = RBT.init<Text, InterCanisterActionResult>();
    var i = 0;
    label l loop {
      if (i >= executingDeleteCanisters.count) break l;
      try {
        let success = await executingDeleteCanisters.elems[i];
        deletedStatusMap := RBT.put<Text, InterCanisterActionResult>(
          deletedStatusMap,
          Text.compare,
          Principal.toText(canisterPrincipals[i]),
          #ok
        );
      } catch (error) {
        deletedStatusMap := RBT.put<Text, InterCanisterActionResult>(
          deletedStatusMap,
          Text.compare,
          Principal.toText(canisterPrincipals[i]),
          #err(Error.message(error))
        );
      };
      Debug.print("completed delete for canisterId=" # debug_show(canisterPrincipals[i]));
      i += 1;
    };

    Debug.print("all deletes complete"); 
    deletedStatusMap;
  };

  public type UpgradePKRangeResult = {
    upgradeCanisterResults: [(Text, InterCanisterActionResult)];
    nextKey: ?Text;
  };

  /// Upgrades up to 5 PK (potentially multiple canisters per PK) at a time in a PK range
  public func upgradeCanistersInPKRange(canisterMap: CanisterMap.CanisterMap, lowerPK: Text, upperPK: Text, limit: Nat, wasmModule: Blob, scalingOptions: CanDB.ScalingOptions): async UpgradePKRangeResult {
    var canisterUpgradeStatusTracker = RBT.init<Text, InterCanisterActionResult>();
    if (limit == 0 ) return {
      upgradeCanisterResults = [];
      nextKey = null;
    };
    let cappedLimit = if (limit < 5) { limit } else { 5 };

    let { results; nextKey } = RBT.scanLimit<Text, CanisterMap.CanisterIdList>(canisterMap, Text.compare, lowerPK, upperPK, #fwd, cappedLimit);
    for ((pk, canisterIdsBuffer) in results.vals()) {
      for (canisterId in canisterIdsBuffer.elems.vals()) {
        try {
          Debug.print("upgrading canister: " # canisterId);
          await CA.upgradeCanisterCode({
            canisterId = Principal.fromText(canisterId);
            wasmModule = wasmModule;
            args = to_candid({
              primaryKey = pk;
              scalingOptions = scalingOptions;
            });
          });
          Debug.print("finished upgrading canister: " # canisterId);
          canisterUpgradeStatusTracker := RBT.put<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker, Text.compare, canisterId, #ok);
        } catch (error) {
          Debug.print("upgrading canister:" # canisterId # " failed with error message=" # Error.message(error));
          canisterUpgradeStatusTracker := RBT.put<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker, Text.compare, canisterId, #err(Error.message(error)) );
        }
      };
    };

    let upgradeCanisterResults = Iter.toArray(RBT.entries<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker));
    let successCanisterIds = Array.mapFilter<(Text, InterCanisterActionResult), Text>(upgradeCanisterResults, func((id, result)) { 
      switch(result) {
        case (#ok) { ?id };
        case (#err(e)) { null };
      }
    });
    let failedCanisterIds = Array.mapFilter<(Text, InterCanisterActionResult), Text>(upgradeCanisterResults, func((id, result)) { 
      switch(result) {
        case (#ok) { null };
        case (#err(e)) { ?id };
      }
    });
    Debug.print("attempted to upgrade " # debug_show(upgradeCanisterResults.size()) # " canisters");
    Debug.print("canister ids=" # debug_show(successCanisterIds) # ", upgraded successfully " # debug_show(successCanisterIds.size()) # " canisters");
    Debug.print("canister ids=" # debug_show(failedCanisterIds) # ", failed to upgrade " # debug_show(failedCanisterIds.size()) # " canisters");

    return {
      upgradeCanisterResults = upgradeCanisterResults;
      nextKey = nextKey;
    };
  };

  /// Upgrades all canisters for a specific PK
  public func upgradeCanistersByPK(canisterMap: CanisterMap.CanisterMap, pk: Text, wasmModule: Blob, scalingOptions: CanDB.ScalingOptions): async [(Text, InterCanisterActionResult)] {
    switch(CanisterMap.get(canisterMap, pk)) {
      case null [];
      case (?canisterIdsBuffer) {
        var canisterUpgradeStatusTracker = RBT.init<Text, InterCanisterActionResult>();
        for (canisterId in canisterIdsBuffer.elems.vals()) {
          try {
            Debug.print("upgrading canister: " # canisterId);
            await CA.upgradeCanisterCode({
              canisterId = Principal.fromText(canisterId);
              wasmModule = wasmModule;
              args = to_candid({
                primaryKey = pk;
                scalingOptions = scalingOptions;
              });
            });
            Debug.print("finished upgrading canister: " # canisterId);
            canisterUpgradeStatusTracker := RBT.put<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker, Text.compare, canisterId, #ok);
          } catch(error) {
            Debug.print("upgrading canister:" # canisterId # " failed with error=" # Error.message(error));
            canisterUpgradeStatusTracker := RBT.put<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker, Text.compare, canisterId, #err(Error.message(error)) );
          };
        };
        Debug.print("pk=" # pk # ", upgrades for all canisters complete");
        return Iter.toArray(RBT.entries<Text, InterCanisterActionResult>(canisterUpgradeStatusTracker));
      }
    };
  };
}