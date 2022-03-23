import Bool "mo:base/Bool";
import Text "mo:base/Text";
import Int "mo:base/Int";
import RBT "mo:stable-rbtree/StableRBTree";

module {

  public type PK = Text; 
  public type SK = Text;
  public type AttributeKey = Text;
  public type AttributeValue = {
    #Text: Text;
    #Int: Int;
    #Bool: Bool;
  };

  public type AttributeMap = RBT.Tree<AttributeKey, AttributeValue>;
  public type Entity = {
    pk: PK;
    sk: SK;
    attributes: AttributeMap;
  };

  public func attributeValuesEqual(av1: AttributeValue, av2: AttributeValue): Bool {
    switch(av1, av2) {
      case(#Text(t1), #Text(t2)) { Text.equal(t1, t2) };
      case(#Int(i1), #Int(i2)) { Int.equal(i1, i2) };
      case(#Bool(b1), #Bool(b2)) { Bool.equal(b1, b2) };
      case _ { false };
    }
  };

  public func createAttributeMapFromPairs(attributePairs: [(AttributeKey, AttributeValue)]): AttributeMap {
    var attributeMap = RBT.init<AttributeKey, AttributeValue>();
    for ((k, v) in attributePairs.vals()) {
      attributeMap := RBT.put<AttributeKey, AttributeValue>(attributeMap, Text.compare, k, v);
    };

    attributeMap;
  };

  public func attributeMapsEqual(m1: AttributeMap, m2: AttributeMap): Bool {
    RBT.equalIgnoreDeleted(m1, m2, Text.equal, attributeValuesEqual);
  };

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

  // TODO: may delete if not used elsewhere or as external dev user helper
  public func createEntity(pk: PK, sk: SK, attributePairs: [(AttributeKey, AttributeValue)]): Entity = {
    pk = pk;
    sk = sk;
    attributes = createAttributeMapFromPairs(attributePairs);
  };
}