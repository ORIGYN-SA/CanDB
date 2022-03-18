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
  };

  public type AttributeMap = RBT.Tree<AttributeKey, AttributeValue>;
  public type Entity = {
    pk: PK;
    sk: SK;
    attributes: AttributeMap;
  };

  // TODO: public func createEntity(pk: PK, sk: SK, Ass)

  func attributeValuesEqual(av1: AttributeValue, av2: AttributeValue): Bool {
    switch(av1, av2) {
      case(#Text(t1), #Text(t2)) { Text.equal(t1, t2) };
      case(#Int(i1), #Int(i2)) { Int.equal(i1, i2) };
      case _ { false };
    }
  };

  public func attributeMapsEqual(m1: AttributeMap, m2: AttributeMap): Bool {
    RBT.equal(m1, m2, Text.equal, attributeValuesEqual);
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
        };

        attributeMapToText(l) # "(k=" # k # ", v=" # value # "), " # attributeMapToText(r) ;
      }
    }
  };
}