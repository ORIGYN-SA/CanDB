import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import Text "mo:base/Text";
import RBT "mo:stable-rbtree/StableRBTree";
import E "../src/Entity";

let { run;test;suite; } = S;

let attributeValuesEqualSuite = suite("attributeValuesEqual",
  [
    test("strings that are equal returns true",
      E.attributeValuesEqual(#text("same"), #text("same")),
      M.equals(T.bool(true))
    ),
    test("strings that are not equal returns false",
      E.attributeValuesEqual(#text("hello"), #text("hello!")),
      M.equals(T.bool(false))
    ),
    test("strings that are both the empty string returns true",
      E.attributeValuesEqual(#text(""), #text("")),
      M.equals(T.bool(true))
    ),
    test("ints that are equal returns true",
      E.attributeValuesEqual(#int(5), #int(5)),
      M.equals(T.bool(true))
    ),
    test("ints that are not equal returns false",
      E.attributeValuesEqual(#int(-5), #int(5)),
      M.equals(T.bool(false))
    ),
    test("ints that are both zero returns true",
      E.attributeValuesEqual(#int(0), #int(0)),
      M.equals(T.bool(true))
    ),
    test("booleans that are equal returns true",
      E.attributeValuesEqual(#bool(false), #bool(false)),
      M.equals(T.bool(true))
    ),
    test("booleans that are not equal returns false",
      E.attributeValuesEqual(#bool(true), #bool(false)),
      M.equals(T.bool(false))
    ),
    test("floats that are equal returns true",
      E.attributeValuesEqual(#float(3.14159), #float(3.14159)),
      M.equals(T.bool(true))
    ),
    test("floats that are not equal returns false",
      E.attributeValuesEqual(#float(3.14159), #float(3.14158)),
      M.equals(T.bool(false))
    ),
    test("text and other type returns false",
      E.attributeValuesEqual(#text("hello"), #float(3.14)),
      M.equals(T.bool(false))
    ),
    test("int and other type returns false",
      E.attributeValuesEqual(#int(1), #bool(true)),
      M.equals(T.bool(false))
    ),
    test("bool and other type returns false",
      E.attributeValuesEqual(#bool(false), #text("")),
      M.equals(T.bool(false))
    ),
    test("float and other type returns false",
      E.attributeValuesEqual(#float(3), #int(3)),
      M.equals(T.bool(false))
    ),
    test("tuples that are equal returns true",
      E.attributeValuesEqual(
        #tuple([#text("hello"), #bool(true), #int(5)]),
        #tuple([#text("hello"), #bool(true), #int(5)]),
      ),
      M.equals(T.bool(true))
    ),
    test("tuples that are not equal returns false",
      E.attributeValuesEqual(
        #tuple([#text("hello"), #bool(true), #int(5)]),
        #tuple([#text("hello"), #bool(true), #int(6)]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty tuples returns true",
      E.attributeValuesEqual(
        #tuple([]),
        #tuple([]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayText that are equal returns true",
      E.attributeValuesEqual(
        #arrayText(["hello", "world", "!"]),
        #arrayText(["hello", "world", "!"]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayText that are not equal returns false",
      E.attributeValuesEqual(
        #arrayText(["hello", "world", "!"]),
        #arrayText(["hello", "world", "?"]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty arrayText returns true",
      E.attributeValuesEqual(
        #arrayText([]),
        #arrayText([]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayInt that are equal returns true",
      E.attributeValuesEqual(
        #arrayInt([1,3,5,2,5]),
        #arrayInt([1,3,5,2,5]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayInt that are not equal returns false",
      E.attributeValuesEqual(
        #arrayInt([1,3,5,2,5]),
        #arrayInt([1,3,10,2,5]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty arrayInt returns true",
      E.attributeValuesEqual(
        #arrayInt([]),
        #arrayInt([]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayBool that are equal returns true",
      E.attributeValuesEqual(
        #arrayBool([false, false, true]),
        #arrayBool([false, false, true]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayBool that are not equal returns false",
      E.attributeValuesEqual(
        #arrayBool([false, false, true]),
        #arrayBool([true, false, true]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty arrayBool returns true",
      E.attributeValuesEqual(
        #arrayBool([]),
        #arrayBool([]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayFloat that are equal returns true",
      E.attributeValuesEqual(
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrayFloat that are not equal returns false",
      E.attributeValuesEqual(
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
        #arrayFloat([0.2, -0.5, 0.5, -3.14159]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty arrayFloat returns true",
      E.attributeValuesEqual(
        #arrayFloat([]),
        #arrayFloat([]),
      ),
      M.equals(T.bool(true))
    ),
    test("trees that are equal returns true",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)])),
          ("aTextKey", #arrayText(["Hello", "world", "!"])),
          ("aIntKey", #arrayInt([10, 5, 1])),
          ("aBoolKey", #arrayBool([true, true, false])),
          ("aFloatKey", #arrayFloat([-0.01, 0.01, 10.559])),
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)])),
          ("aTextKey", #arrayText(["Hello", "world", "!"])),
          ("aIntKey", #arrayInt([10, 5, 1])),
          ("aBoolKey", #arrayBool([true, true, false])),
          ("aFloatKey", #arrayFloat([-0.01, 0.01, 10.559])),
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(true))
    ),
    test("trees that are not equal in a tuple value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.8)]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal in an arrayText value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aTextKey", #arrayText(["Hello", "world", "!"]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aTextKey", #arrayText(["Hello", "world", "?"]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal in an arrayInt value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aIntKey", #arrayInt([10, 5, 1]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aIntKey", #arrayInt([10, 6, 1]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal in an arrayBool value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aBoolKey", #arrayBool([true, true, false]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aBoolKey", #arrayBool([true, true, true]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal in an arrayFloat value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aFloatKey", #arrayFloat([-0.01, 0.01, 10.559])),
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aFloatKey", #arrayFloat([-0.01, 0.01, 10.5591])),
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal in a primitive value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal due to one tree to having different key(s) than the other returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("bKey", #bool(false)),
          ("fKey", #float(3.14159)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are not equal due to having different key names returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("booleanKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(false))
    ),
    test("trees that are equal after having a deletion uses RBT.equalIgnoreDeleted (deleted node remains) and returns true",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        var t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("fKey", #float(3.14159)),
          ("tupKey", #tuple([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        t2 := RBT.delete<Text, E.AttributeValueRBTreeValue>(t2, Text.compare, "fKey");

        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(true))
    ),
  ]
);

let updateAttributeMapWithKVPairsTest = suite("updateAttributeMapWithKVPairs", [
  test("if updating an empty map with no attributes, returns an empty map",
    do {
      let m = E.createAttributeMapFromKVPairs([]);
      let m2 = E.updateAttributeMapWithKVPairs(m, []);
      E.attributeMapsEqual(m, m2);
    },
    M.equals(T.bool(true))
  ),
  test("if updating an empty map with new attributes, returns the updated map",
    do {
      let m = E.createAttributeMapFromKVPairs([]);
      let m2 = E.updateAttributeMapWithKVPairs(m, [
        ("tKey", #text("hello")),
        ("fKey", #float(3.14159)),
      ]);
      let m3 = E.createAttributeMapFromKVPairs([
        ("tKey", #text("hello")),
        ("fKey", #float(3.14159)),
      ]);
      E.attributeMapsEqual(m2, m3);
    },
    M.equals(T.bool(true))
  ),
  test("if updating with no attributes, returns the same map",
    do {
      let m = E.createAttributeMapFromKVPairs([
        ("iKey", #int(10)),
        ("bKey", #bool(false)),
      ]);
      let m2 = E.updateAttributeMapWithKVPairs(m, []);
      E.attributeMapsEqual(m, m2);
    },
    M.equals(T.bool(true))
  ),
  test("if updating with new attributes, returns the updated map",
    do {
      let m = E.createAttributeMapFromKVPairs([
        ("iKey", #int(10)),
        ("bKey", #bool(false)),
      ]);
      let m2 = E.updateAttributeMapWithKVPairs(m, [
        ("tKey", #text("hello")),
        ("fKey", #float(3.14159)),
      ]);
      let m3 = E.createAttributeMapFromKVPairs([
        ("iKey", #int(10)),
        ("bKey", #bool(false)),
        ("tKey", #text("hello")),
        ("fKey", #float(3.14159)),
      ]);
      E.attributeMapsEqual(m2, m3);
    },
    M.equals(T.bool(true))
  ),
  test("if updating existing attributes, returns the correct updated map",
    do {
      let m = E.createAttributeMapFromKVPairs([
        ("iKey", #int(10)),
        ("bKey", #bool(false)),
      ]);
      let m2 = E.updateAttributeMapWithKVPairs(m, [
        ("iKey", #int(9)),
        ("bKey", #bool(true)),
      ]);
      let m3 = E.createAttributeMapFromKVPairs([
        ("iKey", #int(9)),
        ("bKey", #bool(true)),
      ]);
      E.attributeMapsEqual(m2, m3);
    },
    M.equals(T.bool(true))
  ),
]);

run(suite("Entity",
  [
    attributeValuesEqualSuite,
    updateAttributeMapWithKVPairsTest,
  ]
));