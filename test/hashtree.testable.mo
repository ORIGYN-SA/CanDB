import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Text "mo:base/Text";

import E "../src/Entity";
import HT "../src/HashTree";
import RT "../src/RangeTree";
import T "Testable";


/// This module contains Testable typed helpers functions for the tests in HashTreeTest.mo 
module {
  public func testableHashTree(ht: HT.HashTree): T.TestableItem<HT.HashTree> = {
    display = func(ht: HT.HashTree): Text = "";
    equals = func(ht1: HT.HashTree, ht2: HT.HashTree): Bool = HT.equal(ht1, ht2);
    item = ht;
  };

  // Note: just for use in tests - gets all entries in a HashTree (keep this in test and not src b/c can be expensive to run)
  public func entries(ht: HT.HashTree): [E.Entity] {
    let buffer = Buffer.Buffer<E.Entity>(0);

    for (entry in ht.table.vals()) {
      // get all entities for a specific pk (index)
      switch(entry) {
        case null {};
        case (?((pk, rt), tl)) {
          for ((sk, attributeMap) in RT.entries(rt)) {
            buffer.add(
              {
                pk = pk;
                sk = sk;
                attributes = attributeMap;
              }
            )
          }
        }
      }
    };

    Buffer.toArray(buffer);
  };

  public let testableEntity: T.Testable<E.Entity> = {
    display = E.toText;
    equals = E.equal;
  };

  public func testableHashTreeEntries(entityArray: [E.Entity]): T.TestableItem<[E.Entity]> = {
    display = func(entityArray: [E.Entity]): Text {
      var output = "[";
      for (e in entityArray.vals()) {
        output #= E.toText(e) # ",";
      };
      output # "]"
    };
    equals = func(a1: [E.Entity], a2: [E.Entity]): Bool {
      Array.equal(a1, a2, E.equal)
    };
    item = entityArray;
  };

  public func testableHashTreeScanLimitResult(scanLimitResult: ([E.Entity], ?E.SK)): T.TestableItem<([E.Entity], ?E.SK)> = {
    display = func((entityArray: [E.Entity], nextKey: ?E.SK)): Text {
      let nextKeyText = Option.get(nextKey, "null");
      var output = "[";
      for (e in entityArray.vals()) {
        output #= E.toText(e) # ",";
      };
      output # "], nextKey=" # nextKeyText
    };
    equals = func((a1: [E.Entity], nk1: ?E.SK), (a2: [E.Entity], nk2: ?E.SK)): Bool {
      switch(nk1, nk2) {
        case (null, null) { Array.equal(a1, a2, E.equal) };
        case (?k1, ?k2) { Text.equal(k1, k2) and Array.equal(a1, a2, E.equal) };
        case _ { false }
      }
    };
    item = scanLimitResult;
  };

}