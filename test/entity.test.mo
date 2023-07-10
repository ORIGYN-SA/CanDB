import Result "mo:base/Result";
import Text "mo:base/Text";

import RBT "mo:stable-rbtree/StableRBTree";
import {test; suite;} "mo:test";

import E "../src/Entity";


suite("attributeValuesEqual", func() {
    test("strings that are equal returns true", func() {        
        assert E.attributeValuesEqual(#text("same"), #text("same"))
    });
    test("strings that are not equal returns false", func() {
        assert E.attributeValuesEqual(#text("hello"), #text("hello!")) == false
    });
    test("strings that are both the empty string returns true", func() {
        assert E.attributeValuesEqual(#text(""), #text(""))
    });
    test("ints that are equal returns true", func() {
        assert E.attributeValuesEqual(#int(5), #int(5))
    });
    test("ints that are not equal returns false", func() {
        assert E.attributeValuesEqual(#int(-5), #int(5)) == false
    });
    test("ints that are both zero returns true", func() {
        assert E.attributeValuesEqual(#int(0), #int(0))
    });    
    test("booleans that are equal returns true", func() {
        assert E.attributeValuesEqual(#bool(false), #bool(false))
    });
    test("booleans that are not equal returns false", func() {
        assert E.attributeValuesEqual(#bool(true), #bool(false)) == false
    });
    test("floats that are equal returns true", func() {
        assert E.attributeValuesEqual(#float(3.14159), #float(3.14159))
    });
    test("floats that are not equal returns false", func() {
        assert E.attributeValuesEqual(#float(3.14159), #float(3.14158)) == false
    });
    test("text and other type returns false", func() {
        assert E.attributeValuesEqual(#text("hello"), #float(3.14)) == false
    });
    test("int and other type returns false", func() {
        assert E.attributeValuesEqual(#int(1), #bool(true)) == false
    });
    test("bool and other type returns false", func() {
        assert E.attributeValuesEqual(#bool(false), #text("")) == false
    });
    test("float and other type returns false", func() {
        assert E.attributeValuesEqual(#float(3), #int(3)) == false
    });
    test("tuples that are equal returns true", func() {
        assert E.attributeValuesEqual(
        #tuple([#text("hello"), #bool(true), #int(5)]),
        #tuple([#text("hello"), #bool(true), #int(5)]),
      )
    });
    test("tuples that are not equal returns false", func() {
        assert E.attributeValuesEqual(
        #tuple([#text("hello"), #bool(true), #int(5)]),
        #tuple([#text("hello"), #bool(true), #int(6)]),
      ) == false
    });
    test("empty tuples returns true", func() {
        assert E.attributeValuesEqual(
        #tuple([]),
        #tuple([]),
      )
    });
    test("arrayText that are equal returns true", func() {
        assert E.attributeValuesEqual(
        #arrayText(["hello", "world", "!"]),
        #arrayText(["hello", "world", "!"]),
      )
    });
    test("arrayText that are not equal returns false", func() {
        assert E.attributeValuesEqual(
        #arrayText(["hello", "world", "!"]),
        #arrayText(["hello", "world", "?"]),
      ) == false
    });
    test("empty arrayText returns true", func() {
        assert E.attributeValuesEqual(
        #arrayText([]),
        #arrayText([]),
      )
    });
    test("arrayInt that are equal returns true", func() {
        assert E.attributeValuesEqual(
        #arrayInt([1,3,5,2,5]),
        #arrayInt([1,3,5,2,5]),
      )
    });
    test("arrayInt that are not equal returns false", func() {
        assert E.attributeValuesEqual(
        #arrayInt([1,3,5,2,5]),
        #arrayInt([1,3,10,2,5]),
      ) == false
    });
    test("empty arrayInt returns true", func() {
        assert E.attributeValuesEqual(
        #arrayInt([]),
        #arrayInt([]),
      )
    });
    test("arrayBool that are equal returns true", func() {
        assert E.attributeValuesEqual(
        #arrayBool([false, false, true]),
        #arrayBool([false, false, true]),
      )
    });
    test("arrayBool that are not equal returns false", func() {
        assert E.attributeValuesEqual(
        #arrayBool([false, false, true]),
        #arrayBool([true, false, true]),
      ) == false
    });
    test("empty arrayBool returns true", func() {
        assert E.attributeValuesEqual(
        #arrayBool([]),
        #arrayBool([]),
      )
    });
    test("arrayFloat that are equal returns true", func() {
        assert E.attributeValuesEqual(
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
      )
    });
    test("arrayFloat that are not equal returns false", func() {
        assert E.attributeValuesEqual(
        #arrayFloat([0.2, -0.5, 0.5, 3.14159]),
        #arrayFloat([0.2, -0.5, 0.5, -3.14159]),
      ) == false
    });
    test("empty arrayFloat returns true", func() {
        assert E.attributeValuesEqual(
        #arrayFloat([]),
        #arrayFloat([]),
      )
    });
    test("trees that are equal returns true", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2));
            }       
    });
    test("trees that are not equal in a tuple value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal in an arrayText value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal in an arrayInt value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal in an arrayBool value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal in an arrayFloat value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal in a primitive value returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal due to one tree to having different key(s) than the other returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are not equal due to having different key names returns false", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2)) == false;
            }       
    });
    test("trees that are equal after having a deletion uses RBT.equalIgnoreDeleted (deleted node remains) and returns true", func() {
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
                assert E.attributeValuesEqual(#tree(t1), #tree(t2));
            }       
    });    
    
});

suite("updateAttributeMapWithKVPairs", func() {
    test("if updating an empty map with no attributes, returns an empty map", func() {
        do {
                let m = E.createAttributeMapFromKVPairs([]);
                let m2 = E.updateAttributeMapWithKVPairs(m, []);
                assert E.attributeMapsEqual(m, m2);
            }
    });
    test("if updating an empty map with new attributes, returns the updated map", func() {
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
                assert E.attributeMapsEqual(m2, m3);
            }
    });
    test("if updating with no attributes, returns the same map", func() {
        do {
                let m = E.createAttributeMapFromKVPairs([
                    ("iKey", #int(10)),
                    ("bKey", #bool(false)),
                ]);
                let m2 = E.updateAttributeMapWithKVPairs(m, []);
                assert E.attributeMapsEqual(m, m2);
            }
    });
    test("if updating with new attributes, returns the updated map", func() {
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
                assert E.attributeMapsEqual(m2, m3);
            }
    });
    test("if updating existing attributes, returns the correct updated map", func() {
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
                assert E.attributeMapsEqual(m2, m3);
            }
    });
    
});