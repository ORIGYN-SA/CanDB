import CA "../../src/CanisterActions";
import CanDB "../../src/CanDB";
import Entity "../../src/Entity";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Result "mo:base/Result";
import Cycles "mo:base/ExperimentalCycles";
import Prim "mo:⛔";
import Error "mo:base/Error";
import { print; } "mo:base/Debug";

shared ({ caller = owner }) actor class TestService({
  partitionKey: Text;
  scalingOptions: CanDB.ScalingOptions;
  owners: ?[Principal]
}) {
  type CanisterStats = {
    recordsAdded: Nat;
    cycles: Nat;
    entities: Nat;
    heapSize: Nat;
  };

  /// @required (may wrap, but must be present in some form in the canister)
  ///
  /// Initialize CanDB
  stable let db = CanDB.init({
    btreeOrder = null;
    pk = partitionKey;
    scalingOptions = scalingOptions;
  });

  /// @recommended (not required) public API
  public query func getPK(): async Text { db.pk };

  /// @required public API (Do not delete or change)
  public query func skExists(sk: Text): async Bool { 
    CanDB.skExists(db, sk);
  };

  /// @required public API (Do not delete or change)
  public shared({ caller = caller }) func transferCycles(): async () {
    if (caller == owner) {
      await CA.transferCycles(caller);
    };
  };

  public func getDBSize(): async Nat { db.count };

  /// Example of inserting a static entity into CanDB with an sk provided as a parameter
  public func addEntity(sk: Text, i: Int): async Text {
    await* CanDB.put(db, {
      sk = sk;
      attributes = [("i", #int(i))];
    });
    "pk=" # db.pk # ", sk=" # sk;
  };

  public func insertEntities(numEntitiesToInsert: Nat): async Result.Result<CanisterStats, Text> {
    print("insertEntitiesCalled with numEntitiesToInsert=" # debug_show(numEntitiesToInsert));
    print("db.count=" # debug_show(db.count));
    let start = db.count;
    let end = db.count + numEntitiesToInsert;
    var i = db.count;
    let entities = Array.tabulate(numEntitiesToInsert, func (i: Nat): CanDB.PutOptions {
      {
        sk = Nat.toText(i);
        attributes = [("i", #int(i))];
      };
    });
    await* CanDB.batchPut(db, entities);

    let stats = {
      recordsAdded = numEntitiesToInsert;
      cycles = Cycles.balance();
      entities = db.count;
      heapSize = Prim.rts_heap_size();
    };

    print("Inserted " # Nat.toText(numEntitiesToInsert) # " entities into CanDB with total count of " # Nat.toText(db.count) # " entities.");

    #ok(stats);
  }
}