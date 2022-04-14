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
        Text.equal(sk1, sk2) and
        E.attributeMapsEqual(m1, m2);
      });
    };
    item = entries;
  };

  public func testableOptionalAttributeMapWithRangeTreeResult(optMapWithRT: (?E.AttributeMap, RT.RangeTree)): T.TestableItem<(?E.AttributeMap, RT.RangeTree)> = {
    display = func((attributeMap: ?E.AttributeMap, rt: RT.RangeTree)): Text {
      let deletedMapText = Option.getMapped(attributeMap, E.attributeMapToText, "null");
      "attributeMap=" # deletedMapText # ", rt=" # RT.toText(rt);
    };
    equals = func(
      (m1: ?E.AttributeMap, rt1: RT.RangeTree),
      (m2: ?E.AttributeMap, rt2: RT.RangeTree),
    ): Bool {
      switch(m1, m2) {
        case (null, null) { RT.equal(rt1, rt2) };
        case (?m1, ?m2) { E.attributeMapsEqual(m1, m2) and RT.equal(rt1, rt2) };
        case _ { false }
      }
    };
    item = optMapWithRT;
  };

  public func testableRangeTreeScanLimitResult(scanLimitResult: ([(E.SK, E.AttributeMap)], ?E.SK)): T.TestableItem<([(E.SK, E.AttributeMap)], ?E.SK)> = {
    display = func((entries: [(E.SK, E.AttributeMap)], nextKey: ?E.SK)): Text {
      let nextKeyText = Option.get(nextKey, "null");
      var output = "[";
      for ((sk, attributeMap) in Iter.fromArray(entries)) {
        output #= "(" # sk # ", " # E.attributeMapToText(attributeMap) # "),"
      };
      output # "], nextKey=" # nextKeyText;
    };
    equals = func(
      (am1: [(E.SK, E.AttributeMap)], nk1: ?E.SK),
      (am2: [(E.SK, E.AttributeMap)], nk2: ?E.SK),
    ): Bool {
      func skToAttributeMapsEqual((sk1: E.SK, m1: E.AttributeMap), (sk2: E.SK, m2: E.AttributeMap)): Bool {
        Text.equal(sk1, sk2) and E.attributeMapsEqual(m1, m2);
      };
      switch(nk1, nk2) {
        case (null, null) { Array.equal(am1, am2, skToAttributeMapsEqual) };
        case (?k1, ?k2) { Text.equal(k1, k2) and Array.equal(am1, am2, skToAttributeMapsEqual) };
        case _ { false }
      }
    };
    item = scanLimitResult;
  };
}