/// "RangeTreeV2" - a wrapper around a stable BTree that stores the relationship between an Entity's Sort Key and its Attributes

import BT "mo:btree/BTree";
import E "./Entity";
import Buffer "mo:base/Buffer";
import Text "mo:base/Text";
import I "mo:base/Iter";
import Int "mo:base/Int";
import List "mo:base/List";
import Stack "mo:base/Stack";
import Option "mo:base/Option";
import RangeTree "RangeTree";

module {
  /// A RangeTree data structure is a BTree mapping of a Sort Key (Text) to an AttributeMap
  public type RangeTree = BT.BTree<E.SK, E.AttributeMap>;

  /// Initializes an empty RangeTree
  public func init(): RangeTree {
    BT.init<E.SK, E.AttributeMap>(?32);
  };

  /// Returns an entry from the RangeTree based on the sk provided that sk exists in the RangeTree with a non-null AttributeMap
  public func get(rt: RangeTree, sk: E.SK): ?E.AttributeMap {
    BT.get<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  /// Creates or replaces an entry in the RangeTree based upon if the sk of the entity provided exists. Returns the new RangeTree
  public func put(rt: RangeTree, entity: E.Entity): () {
    ignore replace(rt, entity);
  };

  /// Creates or replaces an entry in the RangeTree based upon if the sk of the entity provided exists. Returns the old AttributeMap if the sk existed and the new RangeTree
  public func replace(rt: RangeTree, entity: E.Entity): ?E.AttributeMap {
    BT.insert<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  public func substituteKey(rt: RangeTree, oldKey: E.SK, newKey: E.SK): ?E.AttributeMap {
    BT.substituteKey<E.SK, E.AttributeMap>(rt, Text.compare, oldKey, newKey);
  };

  /// Creates or updates an entry in the RangeTree based upon if the sk of the entity provided exists.
  ///
  /// The updateFunction parameter applies a function that takes null if the entry does not exist, or the current AttributeMap of an existing entry and returns 
  /// a new AttributeMap that is used to update the attributeMap entry. 
  public func update(rt: RangeTree, sk: E.SK, updateFunction: (?E.AttributeMap) -> E.AttributeMap): ?E.AttributeMap {
    BT.update(rt, Text.compare, sk, updateFunction);
  };

  /// Deletes an entry from the RangeTree based upon if the sk of the entity provided exists. Returns the new RangeTree
  public func delete(rt: RangeTree, sk: E.SK): () {
    ignore remove(rt, sk);
  };

  /// Deletes an entry from the RangeTree based upon if the sk of the entity provided exists. Returns the deleted AttributeMap if the sk existed and the new RangeTree
  public func remove(rt: RangeTree, sk: E.SK): ?E.AttributeMap {
    BT.delete<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  type Direction = { #fwd; #bwd };

  /// Performs a in-order scan of the RangeTree between the provided SortKey bounds, returning a number of matching entries in the direction specified, limited by the limit parameter specified in an array formatted as (SK, AttributeMap) for each entry
  public func scanLimit(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat, dir: Direction): BT.ScanLimitResult<E.SK, E.AttributeMap> {
    BT.scanLimit<E.SK, E.AttributeMap>(rt, Text.compare, skLowerBound, skUpperBound, dir, limit);
  };

  // TODO: Decide if this should be public (gauge community feedback)
  /// Not recommended that this is used because it's then more likely that a developer will run into the 2MB egress limit. A limit should
  /// therefore be enforced
  /// Performs a full scan of the RangeTree between the provided Sort Key bounds, returning an array of the matching (SK, AttributeMap) for each entry
  public func scan(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK): [(E.SK, E.AttributeMap)] {
    BT.toArray(rt);
  };

  /// Returns an iterator of all entries in the RangeTree 
  public func entries(rt: RangeTree): I.Iter<(E.SK, E.AttributeMap)> {
    BT.entries<E.SK, E.AttributeMap>(rt);
  };

  /// Performs an equality check between two RangeTrees
  public func equal(rt1: RangeTree, rt2: RangeTree): Bool {
    BT.equals<E.SK, E.AttributeMap>(rt1, rt2, Text.equal, E.attributeMapsEqual);
  };

  /// Mostly for testing/debugging purposes, generates a textual representation of the RangeTree
  public func toText(rt: RangeTree): Text.Text {
    BT.toText<E.SK, E.AttributeMap>(rt, func(t: Text) { t }, E.attributeMapToText);
  };
}