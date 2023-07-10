/// CanisterMap - data structure for keeping track of PK -> CanisterIdList

import I "mo:base/Iter";
import Text "mo:base/Text";

import Buffer "mo:stablebuffer/StableBuffer";
import RBT "mo:stable-rbtree/StableRBTree";

// TODO: Think about modifying this structure to add in the wasm hash of each canister, so can know what version
// of the wasm it is on when performing rolling upgrades

module {
  public type CanisterId = Text;
  public type CanisterIdList = Buffer.StableBuffer<CanisterId>;
  /// CanisterMap is a Red-Black Tree data structure for keeping track of PK -> CanisterIdList
  public type CanisterMap = RBT.Tree<Text, CanisterIdList>; 

  /// Initializes a CanisterMap for storing PK -> CanisterIdList. Must be present in the IndexCanister.
  public func init(): CanisterMap { RBT.init<Text, CanisterIdList>() };

  /// Get list of canister ids for a PK
  public func get(map: CanisterMap, pk: Text): ?CanisterIdList {
    RBT.get<Text, CanisterIdList>(map, Text.compare, pk);
  };

  /// Add (append) a canister id to the CanisterIdList for a PK
  public func add(map: CanisterMap, pk: Text, canisterId: Text): CanisterMap {
    func appendToOrCreateBuffer(existingCanisterIdsForPK: ?CanisterIdList): CanisterIdList {
      let canisterIdsBuffer = switch(existingCanisterIdsForPK) {
        case null { Buffer.initPresized<Text>(1) };
        case (?canisterIdsBuffer) { canisterIdsBuffer }
      };
      Buffer.add<Text>(canisterIdsBuffer, canisterId);
      canisterIdsBuffer;
    };
    let (_, newMap) = RBT.update<Text, CanisterIdList>(map, Text.compare, pk, appendToOrCreateBuffer);
    newMap;
  };

  /// Delete a pk and its associated CanisterIdList
  public func delete(map: CanisterMap, pk: Text): CanisterMap {
    RBT.delete<Text, CanisterIdList>(map, Text.compare, pk);
  };

  /// List all entries of (PK, CanisterIdList)
  public func entries(map: CanisterMap): I.Iter<(Text, CanisterIdList)> { RBT.entries(map) }; 
}