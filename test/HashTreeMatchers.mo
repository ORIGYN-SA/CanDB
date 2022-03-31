import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import HT "../src/HashTree";
import E "../src/Entity";
import RT "../src/RangeTree";
import Buffer "mo:base/Buffer";
import Array "mo:base/Array";

/// This module contains Testable typed helpers functions for the tests in HashTreeTest.mo 
module {
  public func testableHashTree(ht: HT.HashTree): T.TestableItem<HT.HashTree> = {
    display = func(ht: HT.HashTree): Text = "";
    equals = func(ht1: HT.HashTree, ht2: HT.HashTree): Bool = HT.equal(ht1, ht2);
    item = ht;
  };

  // Note: just for use in tests - gets all entries in a HashTree (keep this in test and not src b/c can be expensive to run)
  public func entries(ht: HT.HashTree): [E.Entity] {
    let buffer = Buffer.Buffer<E.Entity>(1);

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

    buffer.toArray();
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
  }
}