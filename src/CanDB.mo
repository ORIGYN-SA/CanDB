/// The CanDB module, containing all methods for initializing and interacting with the CanDB data structure

import RT "./RangeTree";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Bool "mo:base/Bool";
import E "./Entity";

import Debug "mo:base/Debug";
import Error "mo:base/Error";

import Prim "mo:⛔";

module {
  /// Count
  public type ScalingLimitType = { #count: Nat; #heapSize: Nat };

  let HEAP_SIZE_INSERT_LIMIT = 1_250_000_000; // 1.25GB
  let HEAP_SIZE_UPDATE_LIMIT = 1_750_000_000; // 1.75GB

  /// ScalingOptions
  ///
  /// autoScalingCanisterId - The canisterId of the canister in charge of scaling (for now, the index canister)
  /// sizeLimit - the auto-scaling limit for the canister: either limit the auto-scaling by the count or the heap size of the canister
  ///
  /// Once a canister passes the sizeLimit, it will trigger an inter-canister call from the canister storage partition to the
  /// autoScalingCanisterId to spin up a new canister for that partition. Then the scaling status is set to #complete, meaning that 
  /// further insertions (after messages processed in that same consensus round) are sent to the newly spun up canister. However, 
  /// updates to **existing** entity records in a canister storage partition can still happen until the canister hits the 1.75GB 
  /// limit, held via the HEAP_SIZE_UPDATE LIMIT constant.
  public type ScalingOptions = {
    autoScalingCanisterId: Text;
    sizeLimit: ScalingLimitType;
  };
  
  /// CanDB Core 
  ///
  /// pk - the partition key of the CanDB instance. This also corresponds to the PK of the canister storage partition
  /// data - where the data for CanDB is stored
  /// count - the size/count of elements in CanDB (can be used for limit)
  /// scalingOptions - ScalingOptions
  public type DB = {
    pk: E.PK;
    var data: RT.RangeTree;
    var count: Nat;
    var scalingOptions: ScalingOptions;
    // scalingStatus - the scaling status of the canister that this instance of DB is on
    var scalingStatus: { #not_started; #started; #complete };
  };

  public type DBInitOptions = {
    pk: E.PK;
    scalingOptions: ScalingOptions;
  };

  /// initializes a CanDB data structure
  public func init(options: DBInitOptions): DB = {
    pk = options.pk;
    var data = RT.init();
    var count = 0;
    var scalingOptions = options.scalingOptions;
    var scalingStatus = #not_started;
  };

  public type GetOptions = {
    sk: E.SK;
  };

  /// Returns a boolean indicating if an entity with a matching SK is present in CanDB
  public func skExists(db: DB, sk: E.SK): Bool {
    switch(get(db, { sk = sk})) {
      case null { false };
      case _ { true };
    }
  };

  /// Get an entity if it exists in the DB
  public func get(db: DB, options: GetOptions): ?E.Entity {
    switch(RT.get(db.data, options.sk)) {
      case null { null };
      case (?attributeMap) {
        ?{
          pk = db.pk;
          sk = options.sk;
          attributes = attributeMap;
        }
      }
    }
  };

  public type PutOptions = {
    sk: E.SK;
    attributes: [(E.AttributeKey, E.AttributeValue)];
  };

  /// Create an entity or replace an entity if exists in the DB
  /// Auto scales by signaling the index canister to create a new canister with this PK if at capacity
  public func put(db: DB, options: PutOptions): async () {
    ignore replace(db, options);
  };

  public type ReplaceOptions = PutOptions;

  /// Create an entity or replace an entity if exists in the DB, returning the replaced entity
  /// Auto scales by signaling the index canister to create a new canister with this PK if at capacity
  public func replace(db: DB, options: ReplaceOptions): async ?E.Entity {
    let attributeMap = E.createAttributeMapFromKVPairs(options.attributes);
    let (ovAttributeMap, rt) = RT.replace(db.data, E.createEntity(db.pk, options.sk, attributeMap));
    db.data := rt;
    let ov = switch(ovAttributeMap) {
      case null { 
        db.count += 1;
        null;
      };
      case (?map) {
        db.data := rt;
        ?{
          pk = db.pk;
          sk = options.sk;
          attributes = map;
        };
      }
    };

    switch(ov) {
      case null { 
        // Reject new inserts if over the max heap size insert limit limit
        if (db.scalingStatus == #complete and Prim.rts_heap_size() > HEAP_SIZE_INSERT_LIMIT) { 
          Debug.trap("Canister has scaled and surpassed the heap size insert limit");
        }
      };
      case (?originalValue) {
        // Reject new updates if over the max heap size update limit limit
        if (db.scalingStatus == #complete and Prim.rts_heap_size() > HEAP_SIZE_UPDATE_LIMIT) { 
          Debug.trap("[ATTENTION NEEDED]: Canister has scaled and surpassed the heap size update limit. This canister must now be manually repartitioned. Overriding this limit may render your canister unresponsive and result in irrecoverable data loss.");
        }
      }
    };

    await scaleIfAtCapacity(db);
    ov;
  };

  // Logic for if the canister should auto-scale
  func shouldScale(db: DB): Bool {
    switch(db.scalingOptions.sizeLimit) {
      case (#count(limit)) {
        (
          db.count >= limit // count limit passed
          or
          Prim.rts_heap_size() >= HEAP_SIZE_INSERT_LIMIT // heap insert limit passed
        ) 
        and db.scalingStatus == #not_started // and the db for this canister has not yet scaled out
      };
      case (#heapSize(limit)) {
        (
          Prim.rts_heap_size() >= limit // heap size limit passed
          or
          Prim.rts_heap_size() >= HEAP_SIZE_INSERT_LIMIT // heap insert limit passed
        )
        and db.scalingStatus == #not_started // and the db for this canister has not yet scaled out
      };
    }
  };

  // calls scaleCanister if auto-scaling conditions are met
  func scaleIfAtCapacity(db: DB): async () {
    if (shouldScale(db)) {
      // set scalingStatus to #started before the async call so that following messages in the same round
      // that are over the limit/capacity don't spawn additional canisters
      db.scalingStatus := #started;
      await scaleCanister(db);
    }; 
  };

  // scales out a canister
  func scaleCanister(db: DB): async () {
    let indexCanister = actor(db.scalingOptions.autoScalingCanisterId): actor { createAdditionalCanisterForPK: shared (pk: Text) -> async Text };
    try {
      let newCanisterId = await indexCanister.createAdditionalCanisterForPK(db.pk);
      db.scalingStatus := #complete;
      Debug.print("canister creation success for pk=" # db.pk # ", canisterId=" # newCanisterId)
    } catch (err) {
      db.scalingStatus := #not_started;
      Debug.print("error auto-scale creating canister in CanDB: " # Error.message(err));
    };
  };

  public type UpdateOptions = {
    sk: E.SK;
    updateAttributeMapFunction: (?E.AttributeMap) -> E.AttributeMap;
  };

  /// Similar to replace(), but provides the ability to pass a developer defined update function
  /// controlling how specific attributes of the entity are updated on match.
  ///
  /// See the create() and update() functions in examples/simpleDB/src/main.mo, and the tests in
  /// updateSuite() in test/HashTreeTest for some examples of how to use CanDB.update()
  public func update(db: DB, options: UpdateOptions): ?E.Entity {
    let (ovAttributeMap, rt) = RT.update(db.data, options.sk, options.updateAttributeMapFunction);
    switch(ovAttributeMap) {
      case null { null };
      case (?map) {
        db.data := rt;
        ?{
          pk = db.pk;
          sk = options.sk;
          attributes = map;
        }
      }
    }
  };

  public type DeleteOptions = {
    sk: E.SK;
  };

  /// Removes an entity from the DB if exists
  public func delete(db: DB, options: DeleteOptions): () {
    ignore remove(db, options);
  };

  public type RemoveOptions = DeleteOptions;

  /// Remove an entity from the DB and return that entity if exists
  public func remove(db: DB, options: RemoveOptions): ?E.Entity {
    let (removedAttributeMap, rt) = RT.remove(db.data, options.sk);
    switch(removedAttributeMap) {
      case null { null };
      case (?map) {
        db.data := rt;
        db.count -= 1; // TODO: maybe remove this if decide to go with pure heap sizing for auto-scaling
        ?{
          pk = db.pk;
          sk = options.sk;
          attributes = map;
        }
      }
    }
  };

  /// Options passed to scan
  ///
  /// pk - type Text: The Partition Key
  /// skLowerBound - The Sort Key lower bound to scan from (inclusive)
  /// skUpperBound - The Sort Key upper bound to scan from (inclusive)
  /// limit - The limit of entries to scan within the sk bounds at a given time
  /// ascending - Determines the order of results and where scanning will start from, defaults to ascending (starting from the skLowerBound and ending at the skUpperBound)
  public type ScanOptions = {
    skLowerBound: E.SK;
    skUpperBound: E.SK;
    limit: Nat;
    ascending: ?Bool;
  };

  /// Return type of scan()
  ///
  /// entities - array of entities that match the scan
  /// nextKey - next key to be evaluated when the scan limit is hit, is ideal for pagination (i.e. if the limit was hit and the user desires to scan/view more matching results)
  public type ScanResult = {
    entities: [E.Entity];
    nextKey: ?E.SK;
  };

  /// Scans the DB by partition key, a lower/upper bounded sort key range, and a desired result limit
  /// Returns 0 or more items from the db matching the conditions of the ScanOptions passed
  public func scan(db: DB, options: ScanOptions): ScanResult {
    let (skToAttributeMaps, nextKey) = switch(options.ascending) {
      case (?false) { RT.scanLimitReverse(db.data, options.skLowerBound, options.skUpperBound, options.limit) };
      // (?true or null), default to ascending order
      case _ { RT.scanLimit(db.data, options.skLowerBound, options.skUpperBound, options.limit) };
    };
    {
      entities = Array.map<(E.SK, E.AttributeMap), E.Entity>(skToAttributeMaps, func((sk, attributeMap)) { 
        {
          pk = db.pk;
          sk = sk; 
          attributes = attributeMap;
        }
      });
      nextKey = nextKey;
    }
  };
}