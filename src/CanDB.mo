/// The CanDB module, containing all methods for initializing and interacting with the CanDB data structure

import HT "./HashTree";
import RT "./RangeTree";
import Text "mo:base/Text";
import E "./Entity";


module {

  /// The CanDB data structure - an alias for the HashTree data structure (for more, see the HashTree code/documentation)
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

  /// get an entity in the DB if exists
  public func get(db: DB, options: GetOptions): ?E.Entity {
    HT.get(db, options.pk, options.sk);
  };

  public type PutOptions = {
    pk: E.PK;
    sk: E.SK;
    attributes: [(E.AttributeKey, E.AttributeValue)];
  };

  /// create an entity in the DB or replace an entity if exists
  public func put(db: DB, options: PutOptions): () {
    let attributeMap = E.createAttributeMapFromKVPairs(options.attributes);
    HT.put(db, E.createEntity(options.pk, options.sk, attributeMap))
  };

  public type ReplaceOptions = PutOptions;

  /// create an entity in the DB or replace an entity if exists, returning the old entity
  public func replace(db: DB, options: ReplaceOptions): ?E.Entity {
    let attributeMap = E.createAttributeMapFromKVPairs(options.attributes);
    HT.replace(db, E.createEntity(options.pk, options.sk, attributeMap))
  };

  public type DeleteOptions = {
    pk: E.PK;
    sk: E.SK;
  };

  /// remove an entity from the DB
  public func delete(db: DB, options: DeleteOptions): () {
    HT.delete(db, options.pk, options.sk);
  };

  public type RemoveOptions = DeleteOptions;

  /// remove an entity from the DB, return that entity if exists
  public func remove(db: DB, options: RemoveOptions): ?E.Entity {
    HT.remove(db, options.pk, options.sk);
  };

  /// Options passed to scan
  ///
  /// pk - type Text: The Primary Key
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

  /// returns 0 or more items from the db matching the conditions of the ScanOptions passed
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