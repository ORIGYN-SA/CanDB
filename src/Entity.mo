import Bool "mo:base/Bool";
import Text "mo:base/Text";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import RBT "mo:stable-rbtree/StableRBTree";

module {
  /// Primary Key
  public type PK = Text; 
  /// Sort Key
  public type SK = Text;
  /// Attribute Key
  public type AttributeKey = Text;
  /// Attribute Value (Variant)
  public type AttributeValue = {
    #Text: Text;
    #Int: Int;
    #Bool: Bool;
  };

  /// Key to Value mapping of all Entity attributes, stored in a Red-Black Tree
  public type AttributeMap = RBT.Tree<AttributeKey, AttributeValue>;
  
  /// An Entity is the base data record, item, or row that is stored in CanDB.
  ///
  /// It consists of a Primary Key (PK), a Sort Key (SK), and 0 or more attributes
  public type Entity = {
    pk: PK;
    sk: SK;
    attributes: AttributeMap;
  };

  

  /// Creates an AttributeMap Red-Black Tree from an Array of (AttributeKey, AttributeValue)
  public func createAttributeMapFromKVPairs(attributePairs: [(AttributeKey, AttributeValue)]): AttributeMap {
    var attributeMap = RBT.init<AttributeKey, AttributeValue>();
    for ((k, v) in attributePairs.vals()) {
      attributeMap := RBT.put<AttributeKey, AttributeValue>(attributeMap, Text.compare, k, v);
    };

    attributeMap;
  };

  /// Extracts all non-null kv pairs as an Array of (AttributeKey, AttributeValue) from the AttributeMap Red-Black Tree
  /// This is function to aid developers in extracting the AttributeKey and AttributeValue from the AttributeMaps for an Entity that is returned from CanDB 
  public func extractKVPairsFromAttributeMap(attributeMap: AttributeMap): [(AttributeKey, AttributeValue)] {
    Iter.toArray(RBT.entries<AttributeKey, AttributeValue>(attributeMap));
  };

  /// Determines if two AttributeMaps are equal, ignoring deleted values in the AttributeMap Red-Black Tree
  public func attributeMapsEqual(m1: AttributeMap, m2: AttributeMap): Bool {
    RBT.equalIgnoreDeleted(m1, m2, Text.equal, attributeValuesEqual);
  };

  /// Mostly for testing/debugging purposes, generates a textual representation of the AttributeMap and its underlying Red-Black Tree
  public func attributeMapToText(map: AttributeMap): Text.Text {
    switch(map) {
      case (#leaf) { "" };
      case (#node(c, l, (k, v), r)) {
        let color = switch(c) {
          case (#R) { "#R" };
          case (#B) { "#B" }
        };
        let value = switch(v) {
          case null { "null" };
          case (?#Text(t)) { t };
          case (?#Int(i)) { Int.toText(i) };
          case (?#Bool(b)) { Bool.toText(b) };
        };

        attributeMapToText(l) # "(k=" # k # ", v=" # value # "), " # attributeMapToText(r) ;
      }
    }
  };

  /// Creates an Entity from a Primary Key, Sort Key, and Red-Black Tree mapping of Attributes
  public func createEntity(pk: PK, sk: SK, attributeMap: AttributeMap): Entity = {
    pk = pk;
    sk = sk;
    attributes = attributeMap;
  };

  /// Determines if two Entities are equal
  public func equal(e1: Entity, e2: Entity): Bool {
    Text.equal(e1.pk, e2.pk) and 
    Text.equal(e1.sk, e2.sk) and 
    attributeMapsEqual(e1.attributes, e2.attributes);
  };

  /// Mostly for testing/debugging purposes, generates a textual representation of an entity
  public func toText(e: Entity): Text {
    "{ pk=" # e.pk # "; sk=" # e.sk # "; attributes=" # attributeMapToText(e.attributes) # " }"
  };

  func attributeValuesEqual(av1: AttributeValue, av2: AttributeValue): Bool {
    switch(av1, av2) {
      case(#Text(t1), #Text(t2)) { Text.equal(t1, t2) };
      case(#Int(i1), #Int(i2)) { Int.equal(i1, i2) };
      case(#Bool(b1), #Bool(b2)) { Bool.equal(b1, b2) };
      case _ { false };
    }
  };
}