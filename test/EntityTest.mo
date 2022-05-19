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
    test("arrays that are equal returns true",
      E.attributeValuesEqual(
        #array([#text("hello"), #bool(true), #int(5)]),
        #array([#text("hello"), #bool(true), #int(5)]),
      ),
      M.equals(T.bool(true))
    ),
    test("arrays that are not equal returns false",
      E.attributeValuesEqual(
        #array([#text("hello"), #bool(true), #int(5)]),
        #array([#text("hello"), #bool(true), #int(6)]),
      ),
      M.equals(T.bool(false))
    ),
    test("empty arrays returns true",
      E.attributeValuesEqual(
        #array([]),
        #array([]),
      ),
      M.equals(T.bool(true))
    ),
    test("trees that are equal returns true",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(true))
    ),
    test("trees that are not equal in a array value returns false",
      do {
        let t1 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.8)]))
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
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("bKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
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
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("bKey", #bool(false)),
          ("fKey", #float(3.14159)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
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
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        let t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(9)),
          ("booleanKey", #bool(false)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
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
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        var t2 = E.createAttributeValueRBTreeFromKVPairs([
          ("tKey", #text("hello")),
          ("iKey", #int(10)),
          ("bKey", #bool(false)),
          ("fKey", #float(3.14159)),
          ("aKey", #array([#float(0.2), #float(0.4), #float(0.6)]))
        ]);
        t2 := RBT.delete<Text, E.AttributeValueRBTreeValue>(t2, Text.compare, "fKey");

        E.attributeValuesEqual(#tree(t1), #tree(t2));
      },
      M.equals(T.bool(true))
    ),
  ]
);

run(suite("Entity",
  [
    attributeValuesEqualSuite
  ]
));