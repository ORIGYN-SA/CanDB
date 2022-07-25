import CA "mo:candb/CanisterActions";
import CanDB "mo:candb/CanDB";
import Entity "mo:candb/Entity";
import Principal "mo:base/Principal";

shared ({ caller = owner }) actor class UserCanister({
  partitionKey: Text;
  scalingOptions: CanDB.ScalingOptions;
  owners: ?[Principal]
}) {

  /// @required (may wrap, but must be present in some form in the canister)
  ///
  /// Initialize CanDB
  stable let db = CanDB.init({
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

  /// Example of inserting a static entity into CanDB with an sk provided as a parameter
  public func addEntity(sk: Text): async Text {
    await CanDB.put(db, {
      sk = sk;
      attributes = [
      ("name", #text("joe")),
      ("age", #int(24)),
      ("isMember", #bool(true)),
      ];
    });

    "pk=" # db.pk # ", sk=" # sk;
  };
}