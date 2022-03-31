import RBT "mo:stable-rbtree/StableRBTree";
import E "./Entity";
import Text "mo:base/Text";
import I "mo:base/Iter";
import Int "mo:base/Int";
import List "mo:base/List";
import Iter "mo:base/Iter";
import LL "./LinkedList";

module {

  public type RangeTree = RBT.Tree<E.SK, E.AttributeMap>;

  public func init(): RangeTree {
    RBT.init<E.SK, E.AttributeMap>();
  };

  public func get(rt: RangeTree, sk: E.SK): ?E.AttributeMap {
    RBT.get<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  public func replace(rt: RangeTree, entity: E.Entity): (?E.AttributeMap, RangeTree) {
    RBT.replace<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  public func put(rt: RangeTree, entity: E.Entity): RangeTree {
    RBT.put<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  public func delete(rt: RangeTree, sk: E.SK): RangeTree {
    RBT.delete<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  //TODO: write unit tests for this
  public func remove(rt: RangeTree, sk: E.SK): (?E.AttributeMap, RangeTree) {
    RBT.remove<E.SK, E.AttributeMap>(rt, Text.compare, sk)
  };

  public func scan(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK): [(E.SK, E.AttributeMap)] {
    switch(Text.compare(skLowerBound, skUpperBound)) {
      case (#greater) { [] };
      case (#equal) { 
        switch(get(rt, skLowerBound)) {
          case null { [] };
          case (?map) { [(skLowerBound, map)] }
        }
      };
      case (#less) { 
        Iter.toArray(iterScan(rt, skLowerBound, skUpperBound))
      }
    }
  }; 

  public func entries(rt: RangeTree): I.Iter<(E.SK, E.AttributeMap)> {
    RBT.entries<E.SK, E.AttributeMap>(rt);
  };

  public func equal(rt1: RangeTree, rt2: RangeTree): Bool {
    RBT.equalIgnoreDeleted<E.SK, E.AttributeMap>(rt1, rt2, Text.equal, E.attributeMapsEqual);
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
  };

  type IterScanRep = List.List<{ #rt: RangeTree; #kv: (E.SK, ?E.AttributeMap)}>;

  func iterScan(rt: RangeTree, lower: E.SK, upper: E.SK): I.Iter<(E.SK, E.AttributeMap)> {
    object {
      var trees: IterScanRep = ?(#rt(rt), null); 
      public func next() : ?(E.SK, E.AttributeMap) {
        switch(trees) {
          case null { null };
          case (?(#rt(#leaf), rts)) {
            trees := rts;
            next()
          };
          case (?(#kv((sk, attributeMap)), ts)) {
            trees := ts;
            switch (attributeMap) {
              case null { next() };
              case (?map) { ?(sk, map) }
            }
          };
          case (?(#rt(#node(_, l, (sk, attributeMap), r)), rts)) {
            trees := rtAddIfInRange(rts, l, r, (sk, attributeMap));
            next();
          }
        }
      };

      func rtAddIfInRange(rts: IterScanRep, l: RangeTree, r: RangeTree, (sk: E.SK, map: ?E.AttributeMap)): IterScanRep {
        switch(Text.compare(sk, lower), Text.compare(sk, upper)) {
          // value is greater than lower and upper bounds, go left
          case (#greater, #greater) {
            ?(#rt(l), rts);
          };
          // value is greater than lower and equal to upper, go left then append map to tail
          case (#greater, #equal) {
            ?(#rt(l), ?(#kv((sk, map)), rts));
          };
          // value is greater than lower and less than upper, go left, append map, and go right 
          case (#greater, #less) {
            ?(#rt(l), ?(#kv((sk, map)), ?(#rt(r), rts)));
          };
          // value is equal to lower and less than upper, prepend map and go right
          case (#equal, #less) {
            ?(#kv((sk, map)), ?(#rt(r), rts));
          };
          // value is less than lower and upper bounds, go right
          case (#less, #less) {
            ?(#rt(r), rts);
          };
          // the cases where lower bound >= upper bound are covered in the main scan function
          case _ { rts }
        }
      }
    }
  }
}