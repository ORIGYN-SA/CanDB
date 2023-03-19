/// Entity - An entity is the base data record or item that is stored in CanDB

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";
import RBT "mo:stable-rbtree/StableRBTree";

module {
  /// Partition Key
  public type PK = Text; 
  /// Sort Key
  public type SK = Text;
  /// Attribute Key
  public type AttributeKey = Text;

  /// AttributeValue primitive options 
  public type AttributeValuePrimitive = {
    #text: Text;
    #int: Int;
    #bool: Bool;
    #float: Float;
  };

  public type AttributeValueBlob = {
    #blob: Blob;
  };

  /// An AttributeValue can be an array of AttributeValuePrimitive (tuple type)
  public type AttributeValueTuple = {
    #tuple: [AttributeValuePrimitive];
  };

  /// An AttributeValue can be an array of any single one the primitive types (i.e. [Int])
  public type AttributeValueArray = {
    #arrayText: [Text];
    #arrayInt: [Int];
    #arrayBool: [Bool];
    #arrayFloat: [Float];
  };

  public type AttributeValueRBTreeValue = AttributeValuePrimitive or AttributeValueBlob or AttributeValueTuple or AttributeValueArray;

  /// An AttributeValue can be a map (tree) with text keys and values as AttributeValuePrimitive or AttributeValueArray
  public type AttributeValueRBTree = {
    #tree: RBT.Tree<Text, AttributeValueRBTreeValue>;
  };

  /// Attribute Value (Variant). Represents the value of a specific Attribute in an AttributeMap. 
  public type AttributeValue = 
    AttributeValuePrimitive or 
    AttributeValueBlob or
    AttributeValueTuple or
    AttributeValueArray or
    AttributeValueRBTree;
 
  /// Key to Value mapping of all Entity attributes, stored in a Red-Black Tree
  public type AttributeMap = RBT.Tree<AttributeKey, AttributeValue>;
  
  /// An Entity is the base data item or record that is stored in CanDB.
  ///
  /// An entity consists of:
  ///
  /// * Partition Key (PK) - A text/string partition key identifier used to partition your data.
  ///
  /// * Sort Key (SK) - A text/string key identifier used to sort your data. Some examples might be a timestamp, an incrementing identifier, or a numerical value (turned into a string).
  ///
  /// * Attributes - Additional key/value data pertaining to the entity. All attribute keys are of type text/string, and attribute values are expressed as variants, allowing for the dynamic insertion of different types of attribute values.
  ///
  /// The combination of an entity's partition key + sort key is unique in CanDB, meaning only one entity can have the exact same partition key and sort key.
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

  /// Creates an AttributeValueRBTree from an array of key value pairs
  public func createAttributeValueRBTreeFromKVPairs(attributePairs: [(Text, AttributeValueRBTreeValue)]): RBT.Tree<Text, AttributeValueRBTreeValue> {
    var attributeValueRBTree = RBT.init<Text, AttributeValueRBTreeValue>();
    for ((k, v) in attributePairs.vals()) {
      attributeValueRBTree := RBT.put<Text, AttributeValueRBTreeValue>(attributeValueRBTree, Text.compare, k, v);
    };

    attributeValueRBTree;
  };

  /// Updates an AttributeMap Red-Black Tree with an Array of (AttributeKey, AttributeValue)
  public func updateAttributeMapWithKVPairs(attributeMap: AttributeMap, attributePairs: [(AttributeKey, AttributeValue)]): AttributeMap {
    var updatedMap = attributeMap;
    for ((k, v) in attributePairs.vals()) {
      updatedMap := RBT.put<AttributeKey, AttributeValue>(attributeMap, Text.compare, k, v);
    };

    updatedMap; 
  };

  /// Gets a value from AttributeMap corresponding to a specific key
  ///
  ///
  /// ```
  /// let { sk; attributes } = entity;
  /// let userAttributeTree = getAttributeMapValueForKey(attributes, "userAttributes");
  /// ```
  public func getAttributeMapValueForKey(attributeMap: AttributeMap, k: AttributeKey): ?AttributeValue {
    RBT.get(attributeMap, Text.compare, k);
  }; 

  /// Extracts a single value from an AttributeValueRBTree if it exists for the key provided
  ///
  /// example
  /// ```
  /// let { sk; attributes } = entity;
  /// let userAttributeTree = getAttributeMapValueForKey(attributes, "userAttributes");
  /// let userBirthday = switch(userAttributes) {
  ///   case (#tree(attrs)) { getAttributeValueRBTreeValue(attrs, "birthday") };
  ///   case _ { null };
  /// };
  /// ```
  public func getAttributeValueRBTreeValue(attributeValueRBTree: RBT.Tree<Text, AttributeValueRBTreeValue>, k: Text): ?AttributeValueRBTreeValue {
    RBT.get<Text, AttributeValueRBTreeValue>(attributeValueRBTree, Text.compare, k)
  };

  /// Extracts all non-null kv pairs as an Array of (AttributeKey, AttributeValue) from the AttributeMap Red-Black Tree
  /// This is function to aid developers in extracting the AttributeKey and AttributeValue from the AttributeMaps for an Entity that is returned from CanDB 
  public func extractKVPairsFromAttributeMap(attributeMap: AttributeMap): [(AttributeKey, AttributeValue)] {
    Iter.toArray(RBT.entries<AttributeKey, AttributeValue>(attributeMap));
  };

  /// Helper method for extracting all key value pairs from an AttributeValueRBTree.
  ///
  /// Returns an Iterator of all attributes in the inner AttributeValueRBTree
  public func AtributeValueRBTreeToIter(attributeValueRBTree: RBT.Tree<Text, AttributeValueRBTreeValue>): Iter.Iter<(Text, AttributeValueRBTreeValue)> {
    RBT.entries(attributeValueRBTree);
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
          case (?#text(t)) { t };
          case (?#int(i)) { Int.toText(i) };
          case (?#bool(b)) { Bool.toText(b) };
          case (?#float(f)) { Float.toText(f) };
          // TODO: maybe there's a better way to transform a blob to type text for equality purposes?
          case (?#blob(b)) { Nat32.toText(Blob.hash(b)) };
          case (?#tuple(tup)) { debug_show(tup) };
          case (?#arrayText(at)) { debug_show(at) };
          case (?#arrayInt(ai)) { debug_show(ai) };
          case (?#arrayBool(ab)) { debug_show(ab) };
          case (?#arrayFloat(af)) { debug_show(af) };
          case (?#tree(t)) { debug_show(t) };
        };

        attributeMapToText(l) # "(k=" # k # ", v=" # value # "), " # attributeMapToText(r) ;
      }
    }
  };

  /// Creates an Entity from a Partition Key, Sort Key, and Red-Black Tree mapping of Attributes
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

  public func attributeValuesEqual(av1: AttributeValue, av2: AttributeValue): Bool {
    switch(av1, av2) {
      case(#text(t1), #text(t2)) { Text.equal(t1, t2) };
      case(#int(i1), #int(i2)) { Int.equal(i1, i2) };
      case(#bool(b1), #bool(b2)) { Bool.equal(b1, b2) };
      case(#float(f1), #float(f2)) { Float.equalWithin(f1, f2, 1e-10) };
      case(#blob(b1), #blob(b2)) { Blob.equal(b1, b2) };
      case(#tuple(tup1), #tuple(tup2)) { Array.equal<AttributeValue>(tup1, tup2, attributeValuesEqual) };
      case(#arrayText(a1), #arrayText(a2)) { Array.equal<Text>(a1, a2, Text.equal) };
      case(#arrayInt(a1), #arrayInt(a2)) { Array.equal<Int>(a1, a2, Int.equal) };
      case(#arrayBool(a1), #arrayBool(a2)) { Array.equal<Bool>(a1, a2, Bool.equal) };
      case(#arrayFloat(a1), #arrayFloat(a2)) { Array.equal<Float>(a1, a2, func(a, b) { Float.equalWithin(a, b, 1e-10) }) };
      case(#tree(t1), #tree(t2)) { RBT.equalIgnoreDeleted<Text, AttributeValue>(t1, t2, Text.equal, attributeValuesEqual) };
      case _ { false };
    }
  };
}