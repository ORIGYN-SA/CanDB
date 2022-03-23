import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import RT "../src/RangeTree";
import E "../src/Entity";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Iter "mo:base/Iter";
import RTT "./RangeTreeMatchers";

let { run;test;suite; } = S;

//Setup

// Test helper that creates mock attributes
func createMockAttributes(city: Text): E.AttributeMap {
  let attributes = [
    ("state", #Text("OH")),
    ("year", #Int(2020)),
    ("city", #Text(city))
  ];

  E.createAttributeMapFromPairs(attributes);
};

// An AttributeMap value used throughout the tests
let mockAttributes = createMockAttributes("Cleveland"); 

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
        rt := RT.put(rt, { pk = "app1"; sk = "zack"; attributes = createMockAttributes("Columbus")});
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes),
          ("zack", createMockAttributes("Columbus"))
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
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = createMockAttributes("Akron") });
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = createMockAttributes("Motoko!") });
        RT.get(rt, "barry"); 
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?createMockAttributes("Motoko!")))
    )
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
        let (_, newRT) = RT.replace(rt, { pk = "app1"; sk = "zack"; attributes = createMockAttributes("Columbus")});
        Iter.toArray(RT.entries(newRT));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john", mockAttributes),
          ("zack", createMockAttributes("Columbus"))
        ]
      ))
    ),
    test(
      "returns the replaced map if it replaces an entry that already exists",
      do {
        var rt = createRTAndReplaceSKs(["zack", "john"]);
        let (ov, _) = RT.replace(rt, { pk = "app1"; sk = "zack"; attributes = createMockAttributes("Columbus")});
        ov
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?mockAttributes))
    ),
  ]
);

run(suite("RangeTree", 
  [
    initSuite,
    putSuite,
    getSuite,
    deleteSuite,
    replaceSuite
  ]
));

