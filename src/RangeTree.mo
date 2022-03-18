import RBT "mo:stable-rbtree/StableRBTree";
import E "./Entity";
import Text "mo:base/Text";
import I "mo:base/Iter";
import Int "mo:base/Int";

module {

  public type RangeTree = RBT.Tree<E.SK, E.AttributeMap>;

  public func init(): RangeTree {
    RBT.init<E.SK, E.AttributeMap>();
  };

  public func put(rt: RangeTree, entity: E.Entity): RangeTree {
    RBT.put<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  public func get(rt: RangeTree, sk: E.SK): ?E.AttributeMap {
    RBT.get<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  public func entries(rt: RangeTree): I.Iter<(E.SK, E.AttributeMap)> {
    RBT.entries<E.SK, E.AttributeMap>(rt);
  };

  public func equal(rt1: RangeTree, rt2: RangeTree): Bool {
    RBT.equal<E.SK, E.AttributeMap>(rt1, rt2, Text.equal, E.attributeMapsEqual);
  };

  public func toText(rt: RangeTree): Text.Text {
    switch(rt) {
      case (#leaf) { "#leaf" };
      case (#node(c, l, (sk, attrs), r)) {
        let color = switch(c) {
          case (#R) { "#R" };
          case (#B) { "#B" }
        };
        let attributeMap = switch(attrs) {
          case null { "null" };
          case (?map) { E.attributeMapToText(map) }
        };
        "#node(color=" # color # 
        ", l=" # toText(l) 
        # ", {sk=" # sk # ", attributeMap={" # attributeMap # "}, r=" # toText(r) # "}";
      };
    }
  }
}