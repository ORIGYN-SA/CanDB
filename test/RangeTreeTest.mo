import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import RT "../src/RangeTree";
import E "../src/Entity";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Iter "mo:base/Iter";
import RTT "./RangeTreeMatchers";
import TH "./TestHelpers";

let { run;test;suite; } = S;

// Setup

// An AttributeMap value used throughout the tests
let mockAttributes = TH.createMockAttributes("Cleveland"); 

// Test helper that creates a RangeTree, then uses the put function to insert entities with the same PK and attributes,
// but different sks into that RangeTree
func createRTAndPutSKs(sks: [E.SK]): RT.RangeTree {
  let entities = Iter.map<Text, E.Entity>(sks.vals(), func(sk) { { pk = "app1"; sk = sk; attributes = mockAttributes; } });
  var rt = RT.init();
  Iter.iterate<E.Entity>(entities, func(e, _) {
    rt := RT.put(rt, e);
  });

  rt;
};

func createRTAndReplaceSKs(sks: [E.SK]): RT.RangeTree {
  let entities = Iter.map<Text, E.Entity>(sks.vals(), func(sk) { { pk = "app1"; sk = sk; attributes = mockAttributes; } });
  var rt = RT.init();
  Iter.iterate<E.Entity>(entities, func(e, _) {
    let (ov, newRT) = RT.replace(rt, e);
    rt := newRT;
  });

  rt;
};

// Tests 

let initSuite = suite("init", 
  [
    test(
      "creates an empty RBT",
      RT.init(),
      M.equals(RTT.testableRangeTree(
        RBT.init<E.SK, E.AttributeMap>()
      ))
    ),
    test(
      "has a size of 0",
      RBT.size(RT.init()),
      M.equals(T.nat(0))
    )
  ]
);

let putSuite = suite("put",
  [
    test(
      "inserts an item correctly at the root of an empty tree",
      do {
        let rt = createRTAndPutSKs(["john"]);
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes)
        ]
      ))
    ),
    test(
      "entries are inserted into a RBT and ordered by sk regardless of the insertion order",
      do {
        let rt = createRTAndPutSKs(["zack", "brian", "john", "susan", "alice"]);
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("alice", mockAttributes),
          ("brian", mockAttributes),
          ("john", mockAttributes),
          ("susan", mockAttributes),
          ("zack", mockAttributes)
        ]
      ))
    ),
    test(
      "replaces an entry if already exists",
      do {
        var rt = createRTAndPutSKs(["zack", "john"]);
        rt := RT.put(rt, { pk = "app1"; sk = "zack"; attributes = TH.createMockAttributes("Columbus")});
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes),
          ("zack", TH.createMockAttributes("Columbus"))
        ]
      ))
    ),
    test(
      "inserts an entry correctly after it was previously deleted",
      do {
        var rt = createRTAndPutSKs(["zack", "john"]);
        rt := RT.delete(rt, "zack");
        rt := RT.put(rt, { pk = "app1"; sk = "zack"; attributes = TH.createMockAttributes("Columbus")});
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes),
          ("zack", TH.createMockAttributes("Columbus"))
        ]
      ))
    ),
  ]
);

let getSuite = suite("get",
  [
    test("null if called on an empty tree",
      do {
        let map = RT.get(RT.init(), "john"); 
        T.optional(RTT.testableAttributeMap, map);
      },
      M.isNull<E.AttributeMap>()
    ),
    test("not some if called on an empty tree (ensure the isNull matcher works)",
      RT.get(RT.init(), "john"), 
      M.not_(M.isSome<E.AttributeMap>())
    ),
    test("returns the AttributeMap when the entry exists in the tree",
      do {
        let rt = createRTAndPutSKs(["john"]);
        RT.get(rt, "john"); 
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?mockAttributes))
    ),
    test("after an entry was replaced multiple times, returns the most recent",
      do {
        var rt = createRTAndPutSKs(["john", "barry", "silvia"]);
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = TH.createMockAttributes("Akron") });
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = TH.createMockAttributes("Motoko!") });
        RT.get(rt, "barry"); 
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?TH.createMockAttributes("Motoko!")))
    )
  ]
);

let replaceSuite = suite("replace",
  [
    test("inserts an item correctly at the root of an empty tree",
      do {
        let (_, rt) = RT.replace(
          RT.init(), 
          { pk = "app1"; sk = "john"; attributes = mockAttributes }
        ); 
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes)
        ]
      ))
    ),
    test("original value returned is null when inserting an item into an empty tree, as the item does not yet exist",
      do {
        let (ov, _) = RT.replace(
          RT.init(), 
          { pk = "app1"; sk = "john"; attributes = mockAttributes }
        ); 
        T.optional(RTT.testableAttributeMap, ov)
      },
      M.isNull<E.AttributeMap>()
    ),
    test("entries are inserted into a RBT and ordered by sk regardless of the insertion order",
      do {
        let rt = createRTAndReplaceSKs(["zack", "brian", "john", "susan", "alice"]);
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("alice", mockAttributes),
          ("brian", mockAttributes),
          ("john", mockAttributes),
          ("susan", mockAttributes),
          ("zack", mockAttributes)
        ]
      ))
    ),
    test(
      "replaces an entry if already exists",
      do {
        var rt = createRTAndReplaceSKs(["zack", "john"]);
        let (_, newRT) = RT.replace(rt, { pk = "app1"; sk = "zack"; attributes = TH.createMockAttributes("Columbus")});
        Iter.toArray(RT.entries(newRT));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes),
          ("zack", TH.createMockAttributes("Columbus"))
        ]
      ))
    ),
    test(
      "returns the replaced map if it replaces an entry that already exists",
      do {
        var rt = createRTAndReplaceSKs(["zack", "john"]);
        let (ov, _) = RT.replace(rt, { pk = "app1"; sk = "zack"; attributes = TH.createMockAttributes("Columbus")});
        ov
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?mockAttributes))
    ),
  ]
);

let updateSuite = suite("update",
  [
    test("creates a new entry with the correct count if the RT is empty, returning null and the new tree",
      RT.update(RT.init(), "apples", TH.incrementFunc),
      M.equals(
        RTT.testableOptionalAttributeMapWithRangeTreeResult((
          null,
          RT.put(RT.init(), {
            pk = "app1";
            sk = "apples";
            attributes = E.createAttributeMapFromKVPairs([
              ("count", #Int(1))
            ]);
          })
        ))
      )
    ),
    test("creates a new entry with the correct count if the entry was previously deleted in the RT, returning null and the new tree",
      do {
        var rt = createRTAndPutSKs(["apples", "oranges", "muffins"]);
        rt := RT.delete(rt, "apples");
        RT.update(rt, "apples", TH.incrementFunc);
      },
      M.equals(
        RTT.testableOptionalAttributeMapWithRangeTreeResult((
          null,
          RT.put(createRTAndPutSKs(["oranges", "muffins"]), {
            pk = "app1";
            sk = "apples";
            attributes = E.createAttributeMapFromKVPairs([
              ("count", #Int(1))
            ]);
          })
        ))
      )
    ),
    test("updates an existing entry to have a count attribute if that attribute did not exist, returning the old AttributeMap and the new tree",
      RT.update(createRTAndPutSKs(["apples", "oranges", "muffins"]), "apples", TH.incrementFunc),
      M.equals(
        RTT.testableOptionalAttributeMapWithRangeTreeResult((
          ?mockAttributes,
          RT.put(createRTAndPutSKs(["oranges", "muffins"]), {
            pk = "app1";
            sk = "apples";
            attributes = E.createAttributeMapFromKVPairs([
              ("count", #Int(1)),
              ("state", #Text("OH")),
              ("year", #Int(2020)),
              ("city", #Text("Cleveland")),
            ]);
          })
        ))
      )
    ),
    test("updates an existing entry to increment the count attribute if that attribute existed, returning the old AttributeMap and the new tree",
      do {
        var rt = createRTAndPutSKs(["apples", "oranges", "grapes", "frogs", "zuchinni"]);
        rt := RT.put(rt, {
          pk = "app1";
          sk = "shwarma";
          attributes = E.createAttributeMapFromKVPairs([
            ("count", #Int(50)),
            ("state", #Text("CA")),
            ("year", #Int(2021)),
            ("city", #Text("Oakland")),
          ]);
        });
        RT.update(rt, "shwarma", TH.incrementFunc)
      },
      M.equals(
        RTT.testableOptionalAttributeMapWithRangeTreeResult((
          ?E.createAttributeMapFromKVPairs([
            ("count", #Int(50)),
            ("state", #Text("CA")),
            ("year", #Int(2021)),
            ("city", #Text("Oakland")),
          ]),
          RT.put(createRTAndPutSKs(["apples", "oranges", "grapes", "frogs", "zuchinni"]), {
            pk = "app1";
            sk = "shwarma";
            attributes = E.createAttributeMapFromKVPairs([
              ("count", #Int(51)),
              ("state", #Text("CA")),
              ("year", #Int(2021)),
              ("city", #Text("Oakland")),
            ]);
          })
        ))
      )
    ),
  ]
);

let deleteSuite = suite("delete",
  [
    test("deleting from an empty tree returns an empty tree",
      RT.delete(RT.init(), "john"),
      M.equals(RTT.testableRangeTree(
        RBT.init<E.SK, E.AttributeMap>()
      ))
    ),
    test("calling delete from a tree that contained only that single entry, deletes that single entry",
      do {
        let rt = createRTAndPutSKs(["john"]);
        let deletedJohnRT = (RT.delete(rt, "john"));
        Iter.toArray(RT.entries(deletedJohnRT));
      },
      M.equals(RTT.testableRangeTreeEntries([]))
    ),
    test("calling delete with a key that exists in a tree that with multiple entries, deletes that specific entry",
      do {
        let rt = createRTAndPutSKs(["john", "bob", "alice"]);
        let deletedJohnRT = RT.delete(rt, "john");
        Iter.toArray(RT.entries(deletedJohnRT));
      },
      M.equals(RTT.testableRangeTreeEntries([
        ("alice", mockAttributes),
        ("bob", mockAttributes)
      ]))
    ),
    test("calling delete with a key that does not exist in a tree that with multiple entries, does not modify the tree",
      do {
        let rt = createRTAndPutSKs(["john", "bob", "alice"]);
        let deletedZachRT = RT.delete(rt, "zach");
        Iter.toArray(RT.entries(deletedZachRT));
      },
      M.equals(RTT.testableRangeTreeEntries([
        ("alice", mockAttributes),
        ("bob", mockAttributes),
        ("john", mockAttributes)
      ]))
    ),
  ]
);

let removeSuite = suite("remove",
  [
    test("removing from an empty tree returns null and an empty tree",
      RT.remove(RT.init(), "john"),
      M.equals(RTT.testableOptionalAttributeMapWithRangeTreeResult(
        (null, RBT.init<E.SK, E.AttributeMap>())
      ))
    ),
    test("calling remove on a tree that contained only that single entry, removes that single entry, returning the deleted AttributeMap and an empty RangeTree",
      do {
        let rt = createRTAndPutSKs(["john"]);
        RT.remove(rt, "john");
      },
      M.equals(RTT.testableOptionalAttributeMapWithRangeTreeResult(
        (?mockAttributes, RT.init())
      ))
    ),
    test("calling remove with a key that exists in a tree with multiple entries, removes that specific entry, returning the deleted AttributeMap and the updated RangeTree",
      do {
        let rt = createRTAndPutSKs(["john", "bob", "alice"]);
        RT.remove(rt, "john");
      },
      M.equals(RTT.testableOptionalAttributeMapWithRangeTreeResult(
        (?mockAttributes, createRTAndPutSKs(["alice", "bob"]))
      ))
    ),
    test("calling remove with a key that does not exist in a tree that with multiple entries, does not modify the tree, returning null and the original tree",
      do {
        let rt = createRTAndPutSKs(["john", "bob", "alice"]);
        RT.remove(rt, "zach");
      },
      M.equals(RTT.testableOptionalAttributeMapWithRangeTreeResult(
        (null, createRTAndPutSKs(["john", "bob", "alice"]))
      ))
    ),
  ]
);

let scanSuite = suite("scan",
  [
    test("if the Range Tree is empty, returns the empty list",
      RT.scan(RT.init(), "alice", "john"),
      M.equals(RTT.testableRangeTreeEntries([]))
    ),
    test("if the Range Tree contains entries, but none in the specified range, returns the empty list",
      RT.scan(createRTAndPutSKs(["zach"]), "alice", "john"),
      M.equals(RTT.testableRangeTreeEntries([]))
    ),
    test("if the Range Tree contains all entries in the specified range, returns all entries",
      RT.scan(createRTAndPutSKs(["alice", "john", "zach"]), "aa", "zz"),
      M.equals(RTT.testableRangeTreeEntries([
        ("alice", mockAttributes),
        ("john", mockAttributes),
        ("zach", mockAttributes),
      ]))
    ),
    test("if the Range Tree has some entries in the specified range, and some outside of both range bounds, just returns the entries in the range",
      RT.scan(createRTAndPutSKs(["alice", "chris", "john", "molly", "zach"]), "b", "n"),
      M.equals(RTT.testableRangeTreeEntries([
        ("chris", mockAttributes),
        ("john", mockAttributes),
        ("molly", mockAttributes),
      ]))
    ),
    test("if specified range is below entries of the Range Tree, but has some outside the range on the upper end, just returns the entries in the range",
      RT.scan(createRTAndPutSKs(["alice", "chris", "john", "molly", "zach"]), "a", "k"),
      M.equals(RTT.testableRangeTreeEntries([
        ("alice", mockAttributes),
        ("chris", mockAttributes),
        ("john", mockAttributes),
      ]))
    ),
    test("if specified range is above entries of the Range Tree, but has some outside the range on the lower end, just returns the entries in the range",
      RT.scan(createRTAndPutSKs(["alice", "chris", "john", "molly", "zach"]), "l", "zz"),
      M.equals(RTT.testableRangeTreeEntries([
        ("molly", mockAttributes),
        ("zach", mockAttributes),
      ]))
    ),
    test("if specified range bounds match specific entries exactly, returns the entries in the range including the bounds",
      RT.scan(createRTAndPutSKs(["alice", "chris", "john", "molly", "zach"]), "chris", "molly"),
      M.equals(RTT.testableRangeTreeEntries([
        ("chris", mockAttributes),
        ("john", mockAttributes),
        ("molly", mockAttributes),
      ]))
    ),
  ]
);

let scanLimitSuite = suite("scanLimit",
  [
    test("if the limit is 0, returns the empty list even if items are within the bounds, and the appropriate nextKey",
      RT.scanLimit(createRTAndPutSKs(["bruce", "molly"]), "b", "y", 0),
      M.equals(RTT.testableRangeTreeScanLimitResult([], ?"bruce"))
    ),
    test("if the RangeTree is empty, returns the empty list and null nextKey",
      RT.scanLimit(RT.init(), "a", "z", 1),
      M.equals(RTT.testableRangeTreeScanLimitResult([], null))
    ),
    test("if the RangeTree does not contain keys in the bounds, returns the empty list and null nextKey",
      RT.scanLimit(createRTAndPutSKs(["alice", "zach"]), "b", "y", 1),
      M.equals(RTT.testableRangeTreeScanLimitResult([], null))
    ),
    test("if the RangeTree contains only keys inside the bounds and fewer keys in the bounds than the limit, returns all the keys in the RangeTree in order and null nextKey",
      RT.scanLimit(createRTAndPutSKs(["bruce", "molly"]), "b", "y", 3),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("bruce", mockAttributes),
        ("molly", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys both inside and outside the bounds and fewer keys in the bounds than the limit, returns all the keys in the RangeTree inside the bounds in order and null nextKey",
      RT.scanLimit(createRTAndPutSKs(["alice", "bruce", "molly", "zach"]), "b", "y", 3),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("bruce", mockAttributes),
        ("molly", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains more keys in the bounds than the limit, returns the number of keys in the bounds equal to the limit in order and the appropriate nextKey",
      RT.scanLimit(createRTAndPutSKs(["alice", "bruce", "carol", "john", "molly", "nancy", "sam", "tom"]), "a", "z", 5),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("alice", mockAttributes),
        ("bruce", mockAttributes),
        ("carol", mockAttributes),
        ("john", mockAttributes),
        ("molly", mockAttributes),
      ], ?"nancy"))
    ),
    test("if the RangeTree contains keys equal to, inside, and outside of the bounds and fewer keys in the inclusive bounds than the limit, returns all the keys in RangeTree in the inclusive bounds in order and null nextKey",
      RT.scanLimit(createRTAndPutSKs(["a", "b", "d", "m", "y", "z"]), "b", "y", 10),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("b", mockAttributes),
        ("d", mockAttributes),
        ("m", mockAttributes),
        ("y", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys equal to, inside, and outside of the bounds and more keys in the inclusive bounds than the limit, returns the number of keys in the inclusive bounds equal to the limit in order and the appropriate nextKey",
      RT.scanLimit(createRTAndPutSKs(["a", "b", "d", "m", "y", "z"]), "b", "y", 2),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("b", mockAttributes),
        ("d", mockAttributes),
      ], ?"m"))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree, returns the expected result and null nextKey",
      RT.scanLimit(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "c",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("c", mockAttributes),
        ("d", mockAttributes),
        ("e", mockAttributes),
        ("f", mockAttributes),
        ("g", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys in the bounds on the left (lower) side of the tree and reaches the limit, returns the expected result and the appropriate nextKey",
      RT.scanLimit(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "c",
        "g",
        3
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("c", mockAttributes),
        ("d", mockAttributes),
        ("e", mockAttributes),
      ], ?"f"))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree and some of the keys are deleted, returns the expected result and null nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "i");
          rt := RT.delete(rt, "a");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "c",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("c", mockAttributes),
        ("e", mockAttributes),
        ("g", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree and some of the keys are deleted including the lower bound result, returns the expected result and null nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "i");
          rt := RT.delete(rt, "a");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "d",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("e", mockAttributes),
        ("g", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree, returns the expected result and null nextKey",
      RT.scanLimit(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "r",
        "v",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("r", mockAttributes),
        ("s", mockAttributes),
        ("t", mockAttributes),
        ("u", mockAttributes),
        ("v", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys in the bounds on the right (upper) side of the tree and reaches the limit, returns the expected result and appropriate nextKey",
      RT.scanLimit(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "r",
        "v",
        3
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("r", mockAttributes),
        ("s", mockAttributes),
        ("t", mockAttributes),
      ], ?"u"))
    ),
    test("if the RangeTree contains the # of keys equal to the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted, returns the expected result and null nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "k",
        "r",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("k", mockAttributes),
        ("n", mockAttributes),
        ("o", mockAttributes),
        ("p", mockAttributes),
        ("q", mockAttributes),
        ("r", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower bound result, returns the expected result and null nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "j",
        "r",
        10
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("k", mockAttributes),
        ("n", mockAttributes),
        ("o", mockAttributes),
        ("p", mockAttributes),
        ("q", mockAttributes),
        ("r", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower and upper bound result, returns the expected result and null nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "k");
          RT.delete(rt, "r");
        },
        "k",
        "r",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("n", mockAttributes),
        ("o", mockAttributes),
        ("p", mockAttributes),
        ("q", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains more keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower and upper bound result, returns the expected result and appropriate nextKey",
      RT.scanLimit(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "k");
          RT.delete(rt, "r");
        },
        "k",
        "r",
        2
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("n", mockAttributes),
        ("o", mockAttributes),
      ], ?"p"))
    ),
  ]
);

let scanLimitReverseSuite = suite("scanLimitReverse",
  [
    test("if the limit is 0, returns the empty list even if items are within the bounds and the appropriate nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["bruce", "molly"]), "b", "y", 0),
      M.equals(RTT.testableRangeTreeScanLimitResult([], ?"molly"))
    ),
    test("if the RangeTree is empty, returns the empty list and null nextKey",
      RT.scanLimitReverse(RT.init(), "a", "z", 1),
      M.equals(RTT.testableRangeTreeScanLimitResult([], null))
    ),
    test("if the RangeTree does not contain keys in the bounds, returns the empty list and null nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["alice", "zach"]), "b", "y", 1),
      M.equals(RTT.testableRangeTreeScanLimitResult([], null))
    ),
    test("if the RangeTree contains only keys inside the bounds and fewer keys in the bounds than the limit, returns all the keys in the RangeTree in descending order and null nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["bruce", "molly"]), "b", "y", 3),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("molly", mockAttributes),
        ("bruce", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys both inside and outside the bounds and fewer keys in the bounds than the limit, returns all the keys in the RangeTree inside the bounds in descending order and null nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["alice", "bruce", "molly", "zach"]), "b", "y", 3),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("molly", mockAttributes),
        ("bruce", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains more keys in the bounds than the limit, returns the number of keys in the bounds equal to the limit in descending order and the appropriate nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["alice", "bruce", "carol", "john", "molly", "nancy", "sam", "tom"]), "a", "z", 5),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("tom", mockAttributes),
        ("sam", mockAttributes),
        ("nancy", mockAttributes),
        ("molly", mockAttributes),
        ("john", mockAttributes),
      ], ?"carol"))
    ),
    test("if the RangeTree contains keys equal to, inside, and outside of the bounds and fewer keys in the inclusive bounds than the limit, returns all the keys in RangeTree in the inclusive bounds in descending order and null nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["a", "b", "d", "m", "y", "z"]), "b", "y", 10),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("y", mockAttributes),
        ("m", mockAttributes),
        ("d", mockAttributes),
        ("b", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys equal to, inside, and outside of the bounds and more keys in the inclusive bounds than the limit, returns the number of keys in the inclusive bounds equal to the limit in descending order and the appropriate nextKey",
      RT.scanLimitReverse(createRTAndPutSKs(["a", "b", "d", "m", "y", "z"]), "b", "y", 2),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("y", mockAttributes),
        ("m", mockAttributes),
      ], ?"d"))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "c",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("g", mockAttributes),
        ("f", mockAttributes),
        ("e", mockAttributes),
        ("d", mockAttributes),
        ("c", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys in the bounds on the left (lower) side of the tree and reaches the limit, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "c",
        "g",
        3
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("g", mockAttributes),
        ("f", mockAttributes),
        ("e", mockAttributes),
      ], ?"d"))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree and some of the keys are deleted, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "i");
          rt := RT.delete(rt, "a");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "c",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("g", mockAttributes),
        ("e", mockAttributes),
        ("c", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the left (lower) side of the tree and some of the keys are deleted including the lower bound result, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "i");
          rt := RT.delete(rt, "a");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "d",
        "g",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("g", mockAttributes),
        ("e", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "r",
        "v",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("v", mockAttributes),
        ("u", mockAttributes),
        ("t", mockAttributes),
        ("s", mockAttributes),
        ("r", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains keys in the bounds on the right (upper) side of the tree and reaches the limit, returns the expected result and appropriate nextKey",
      RT.scanLimitReverse(
        createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]), 
        "r",
        "v",
        3
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("v", mockAttributes),
        ("u", mockAttributes),
        ("t", mockAttributes),
      ], ?"s"))
    ),
    test("if the RangeTree contains the number of keys equal to the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "k",
        "r",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("r", mockAttributes),
        ("q", mockAttributes),
        ("p", mockAttributes),
        ("o", mockAttributes),
        ("n", mockAttributes),
        ("k", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower bound result, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "d");
          RT.delete(rt, "f");
        },
        "j",
        "r",
        10
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("r", mockAttributes),
        ("q", mockAttributes),
        ("p", mockAttributes),
        ("o", mockAttributes),
        ("n", mockAttributes),
        ("k", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains less keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower and upper bound result, returns the expected result and null nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "k");
          RT.delete(rt, "r");
        },
        "k",
        "r",
        6
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("q", mockAttributes),
        ("p", mockAttributes),
        ("o", mockAttributes),
        ("n", mockAttributes),
      ], null))
    ),
    test("if the RangeTree contains more keys than the limit in the bounds on the right (upper) side of the tree and some of the keys are deleted including the lower and upper bound result, returns the expected result and appropriate nextKey",
      RT.scanLimitReverse(
        do {
          var rt = createRTAndPutSKs(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]); 
          rt := RT.delete(rt, "h");
          rt := RT.delete(rt, "j");
          rt := RT.delete(rt, "t");
          rt := RT.delete(rt, "l");
          rt := RT.delete(rt, "m");
          rt := RT.delete(rt, "s");
          rt := RT.delete(rt, "k");
          RT.delete(rt, "r");
        },
        "k",
        "r",
        2
      ),
      M.equals(RTT.testableRangeTreeScanLimitResult([
        ("q", mockAttributes),
        ("p", mockAttributes),
      ], ?"o"))
    ),
  ]
);

run(suite("RangeTree", 
  [
    initSuite,
    putSuite,
    getSuite,
    replaceSuite,
    updateSuite,
    deleteSuite,
    removeSuite,
    scanSuite,
    scanLimitSuite,
    scanLimitReverseSuite,
  ]
));

