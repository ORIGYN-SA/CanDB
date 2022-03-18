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
  var attributeMap = RBT.init<E.AttributeKey, E.AttributeValue>();
  for ((k, v) in attributes.vals()) {
    attributeMap := RBT.put<E.AttributeKey, E.AttributeValue>(attributeMap, Text.compare, k, v);
  };

  attributeMap;
};

// An AttributeMap value used throughout the tests
let mockAttributes = createMockAttributes("Cleveland"); 

// Test helper that creates a RangeTree, then inserts entities with the same PK and attributes, but different sks into that RangeTree
func createRTWithSKs(sks: [E.SK]): RT.RangeTree {
  let entities = Iter.map<Text, E.Entity>(sks.vals(), func(sk) { { pk = "app1"; sk = sk; attributes = mockAttributes; } });
  var rt = RT.init();
  Iter.iterate<E.Entity>(entities, func(e, _) {
    rt := RT.put(rt, e);
  });

  rt;
};

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
        let rt = createRTWithSKs(["john_10928"]);
        Iter.toArray(RT.entries(rt));
      },
      M.equals(RTT.testableRangeTreeEntries(
        [
          ("john_10928", mockAttributes)
        ]
      ))
    ),
    test(
      "entries are inserted into a RBT and ordered by sk regardless of the insertion order",
      do {
        let rt = createRTWithSKs(["zack", "brian", "john", "susan", "alice"]);
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
        var rt = createRTWithSKs(["zack", "john"]);
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
        let rt = createRTWithSKs(["john"]);
        RT.get(rt, "john"); 
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?mockAttributes))
    ),
    test("after an entry was replaced multiple times, returns the most recent",
      do {
        var rt = createRTWithSKs(["john", "barry", "silvia"]);
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = createMockAttributes("Akron") });
        rt := RT.put(rt, { pk = "app1"; sk = "barry"; attributes = createMockAttributes("Motoko!") });
        RT.get(rt, "barry"); 
      },
      M.equals<?E.AttributeMap>(T.optional(RTT.testableAttributeMap, ?createMockAttributes("Motoko!")))
    )
  ]
);

run(suite("RangeTree", 
  [
    initSuite,
    putSuite,
    getSuite,
  ]
));

