/// Utility functions for the CanDB Index Canister that also help reduce some boilerplate

import Principal "mo:base/Principal";

import CanisterMap "./CanisterMap";

module {
  /// Authorization Helper function used for auto-scaling that determines if the calling canister has the same PK as
  /// the canister that is about to be spun up
  public func callingCanisterOwnsPK(caller: Principal, canisterMap: CanisterMap.CanisterMap, pk: Text): Bool {
    switch(CanisterMap.get(canisterMap, pk)) {
      case null { false };
      case (?canisterIdsBuffer) {
        for (canisterId in canisterIdsBuffer.elems.vals()) {
          if (Principal.toText(caller) == canisterId) {
            return true;
          }
        };
        return false;
      }
    }
  }; 
}