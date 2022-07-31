/// The CanDB module, containing all methods for initializing and interacting with the CanDB data structure

import HT "./HashTree";
import RT "./RangeTree";
import Text "mo:base/Text";
import E "./Entity";


module {

  /// The CanDB data structure - an alias for the HashTree data structure (for more, see the HashTree code/documentation)
  // TODO: Re-work this data structure to be a RangeTree under a RangeTree (since HashMaps are not performant)
  public type DB = HT.HashTree;

  /// initializes a CanDB data structure
  public func init(): DB {
    HT.init();
  };

  /// initializes a CanDB data structure of specific pk size
  /// Note: use this only when you know the starting size of your DB by the number of distinct pks
  public func initPreSized(initCapacity: Nat): DB {
    HT.initPreSized(initCapacity);
  };

  public type GetOptions = {
    pk: E.PK;
    sk: E.SK;
  };

  /// Get an entity if exists in the DB
  public func get(db: DB, options: GetOptions): ?E.Entity {
    HT.get(db, options.pk, options.sk);
  };

  public type PutOptions = {
    pk: E.PK;
    sk: E.SK;
    attributes: [(E.AttributeKey, E.AttributeValue)];
  };

  /// Create an entity or replace an entity if exists in the DB
  public func put(db: DB, options: PutOptions): () {
    let attributeMap = E.createAttributeMapFromKVPairs(options.attributes);
    HT.put(db, E.createEntity(options.pk, options.sk, attributeMap))
  };

  public type ReplaceOptions = PutOptions;

  /// Create an entity or replace an entity if exists in the DB, returning the replaced entity
  public func replace(db: DB, options: ReplaceOptions): ?E.Entity {
    let attributeMap = E.createAttributeMapFromKVPairs(options.attributes);
    HT.replace(db, E.createEntity(options.pk, options.sk, attributeMap))
  };

  public type UpdateOptions = {
    pk: E.PK;
    sk: E.SK;
    updateAttributeMapFunction: (?E.AttributeMap) -> E.AttributeMap;
  };

  /// Similar to replace(), but provides the ability to pass a developer defined update function
  /// controlling how specific attributes of the entity are updated on match.
  ///
  /// See the create() and update() functions in examples/simpleDB/src/main.mo, and the tests in
  /// updateSuite() in test/HashTreeTest for some examples of how to use CanDB.update()
  public func update(db: DB, options: UpdateOptions): ?E.Entity {
    HT.update(db, options.pk, options.sk, options.updateAttributeMapFunction)
  };

  public type DeleteOptions = {
    pk: E.PK;
    sk: E.SK;
  };

  /// Removes an entity from the DB if exists
  public func delete(db: DB, options: DeleteOptions): () {
    HT.delete(db, options.pk, options.sk);
  };

  public type RemoveOptions = DeleteOptions;

  /// Remove an entity from the DB and return that entity if exists
  public func remove(db: DB, options: RemoveOptions): ?E.Entity {
    HT.remove(db, options.pk, options.sk);
  };

  /// Options passed to scan
  ///
  /// pk - type Text: The Partition Key
  /// skLowerBound - The Sort Key lower bound to scan from (inclusive)
  /// skUpperBound - The Sort Key upper bound to scan from (inclusive)
  /// limit - The limit of entries to scan within the sk bounds at a given time
  /// ascending - Determines the order of results and where scanning will start from, defaults to ascending (starting from the skLowerBound and ending at the skUpperBound)
  public type ScanOptions = {
    pk: E.PK;
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
    let (entities, nextKey) = switch(options.ascending) {
      case (?false) { HT.scanLimitReverse(db, options.pk, options.skLowerBound, options.skUpperBound, options.limit) };
      // (?true or null), default to ascending order
      case _ { HT.scanLimit(db, options.pk, options.skLowerBound, options.skUpperBound, options.limit) };
    };
    {
      entities = entities;
      nextKey = nextKey;
    }
  };
}