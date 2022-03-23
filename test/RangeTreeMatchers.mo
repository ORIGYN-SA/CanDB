import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import RT "../src/RangeTree";
import E "../src/Entity";
import Option "mo:base/Option";
import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Text "mo:base/Text";

/// This module contains Testable typed helpers functions for the tests in RangeTreeTest.mo
module {
  public func testableRangeTree(rt: RT.RangeTree): T.TestableItem<RT.RangeTree> = {
    display = func(rt: RT.RangeTree): Text = RT.toText(rt);
    equals = func(rt1: RT.RangeTree, rt2: RT.RangeTree): Bool {
      RT.equal(rt1, rt2);
    };
    item = rt;
  };

  public let testableAttributeMap: T.Testable<E.AttributeMap> = {
    display = func(attributeMap: E.AttributeMap): Text = E.attributeMapToText(attributeMap);
    equals = E.attributeMapsEqual;
  };

  public func testableRangeTreeEntries(entries: [(E.SK, E.AttributeMap)]): T.TestableItem<[(E.SK, E.AttributeMap)]> = {
    display = func(entries: [(E.SK, E.AttributeMap)]): Text {
      var output = "[";
      for ((sk, attributeMap) in Iter.fromArray(entries)) {
        output #= "(" # sk # ", " # E.attributeMapToText(attributeMap) # "),"
      };
      output # "]";
    };
    equals = func(
      expectedAttributeMaps: [(E.SK, E.AttributeMap)], 
      actualAttributeMaps: [(E.SK, E.AttributeMap)]
    ): Bool {
      Array.equal<(E.SK, E.AttributeMap)>(expectedAttributeMaps, actualAttributeMaps, func((sk1, m1), (sk2, m2)) {
        E.attributeMapsEqual(m1, m2);
      });
    };
    item = entries;
  };

  // TODO: delete if not used - This might come in handy for external testing?
  public func atSKMatches(sk: T.TestableItem<E.SK>, matcher: M.Matcher<E.AttributeMap>): M.Matcher<RT.RangeTree> = {
    matches = func(rt: RT.RangeTree): Bool {
      Option.getMapped(RT.get(rt, sk.item), matcher.matches, false);
    };
    describeMismatch = func(rt: RT.RangeTree, description: M.Description) {
      switch(RT.get(rt, sk.item)) {
        case null {
          description.appendText("Missing sk " # sk.display(sk.item));
        };
        case (?map) {
          matcher.describeMismatch(map, description);
        };
      }
    }
  } 
}