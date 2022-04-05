import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import RT "../src/RangeTree";
import HT "../src/HashTree";
import HTM "./HashTreeMatchers";
import HM "mo:stable-hash-map/FunctionalStableHashMap";
import E "../src/Entity";
import TH "./TestHelpers";

let { run;test;suite; } = S;

let initSuite = suite("init", 
  [
    test("initializes a hashmap of type E.PK, RangeTree",
      HT.init(),
      M.equals(HTM.testableHashTree(
        HM.init<E.PK, RT.RangeTree>()
      ))
    ),
    test("has a size of 0",
      HM.size<E.PK, RT.RangeTree>(HT.init()),
      M.equals(T.nat(0))
    )
  ]
);

let mockAttributes = TH.createMockAttributes("Cleveland");

let putSuite = suite("put", 
  [
    test("inserts an item into an empty HashTree",
      do {
        let ht = HT.init();
        HT.put(ht, {
          pk = "app1";
          sk = "john";
          attributes = mockAttributes; 
        });
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app1"; sk = "john"; attributes = mockAttributes }
      ]))
    ),
    test("inserts items with different pks into a HashTree",
      do {
        HTM.entries(
          TH.createHashTreeWithPKSKMockEntries([
            ("app1", "john"),
            ("app2", "dave"),
            ("app3", "shelly"),
          ])
        )
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "dave"; attributes = mockAttributes },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
      ]))
    ),
    test("inserts items with different pks and multiple sks per pk into a HashTree, and the items entries are grouped in sk order by pk",
      do {
        HTM.entries(
          TH.createHashTreeWithPKSKMockEntries([
            ("app1", "john"),
            ("app1", "steve"),
            ("app1", "clara"),
            ("app2", "dave"),
            ("app2", "abigail"),
            ("app3", "shelly"),
            ("app3", "bruce"),
            ("app3", "gail"),
            ("app4", "shawn"),
          ])
        )
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app1"; sk = "clara"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
        { pk = "app1"; sk = "steve"; attributes = mockAttributes },
        { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
        { pk = "app2"; sk = "dave"; attributes = mockAttributes },
        { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
        { pk = "app3"; sk = "gail"; attributes = mockAttributes },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app4"; sk = "shawn"; attributes = mockAttributes },
      ]))
    ),
    test("replaces an entry if it already exists at the root of an sk's RangeTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app2", "dave"),
          ("app3", "shelly"),
        ]);
        HT.put(ht, { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") });
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
      ]))
    ),
    test("replaces an entry if it exists deep in a sk's RangeTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "dave"),
          ("app1", "shelly"),
          ("app1", "alice"),
          ("app1", "bruce"),
          ("app1", "abigail"),
        ]);
        HT.put(ht, { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") });
        HT.put(ht, { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") });
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app1"; sk = "abigail"; attributes = mockAttributes },
        { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") },
        { pk = "app1"; sk = "bruce"; attributes = mockAttributes },
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") },
      ]))
    ),
  ]
);

let getSuite = suite("get",
  [
    test("returns null if used on an empty HashTree",
      do {
        let entity = HT.get(HT.init(), "app1", "john");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("returns null if the pk does not exist",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app2", "shelly"),
          ("app3", "dave"),
        ]);
        let entity = HT.get(ht, "app1", "john");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("returns null if the sk does not exist",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        let entity = HT.get(ht, "app1", "john");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("returns the entity if it exists and is the only item in the hashtree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
        ]);
        HT.get(ht, "app1", "dave");
      },
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app1";
        sk = "dave";
        attributes = TH.createMockAttributes("Cleveland")
      }))
    ),
    test("returns the entity if it exists in a range tree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
          ("app1", "john"),
          ("app1", "barry"),
          ("app1", "alice"),
          ("app2", "barry"),
        ]);
        HT.get(ht, "app1", "barry");
      },
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app1";
        sk = "barry";
        attributes = TH.createMockAttributes("Cleveland")
      }))
    ),
  ]
);

let removeSuite = suite("remove",
  [
    test("remove on an empty HashTree returns null",
      do {
        let ht = HT.init();
        let entity = HT.remove(ht, "app1", "john");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("remove on an empty HashTree does not modify the HashTree",
      do {
        let ht = HT.init();
        let entity = HT.remove(ht, "app1", "john");
        ht;
      },
      M.equals(HTM.testableHashTree(HT.init()))
    ),
    test("remove on a HashTree that does not contain the pk returns null",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        let entity = HT.remove(ht, "app2", "shelly");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("remove on a HashTree that contains the pk, but not the sk returns null",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        let entity = HT.remove(ht, "app1", "john");
        T.optional<E.Entity>(HTM.testableEntity, entity);
      },
      M.isNull<E.Entity>()
    ),
    test("remove on a HashTree that contains the pk and sk returns the removed entry",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        HT.remove(ht, "app1", "dave");
      },
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app1";
        sk = "dave";
        attributes = TH.createMockAttributes("Cleveland");
      }))
    ),
    test("remove on a HashTree that contains the pk, but not the sk does not modify the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        let entity = HT.remove(ht, "app1", "john");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
      ]))
    ),
    test("remove on a HashTree with one entry that contains the pk and sk removes that entry from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
        ]);
        let _ = HT.remove(ht, "app1", "dave");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([]))
    ),
    test("remove on a HashTree with multiple different pk that contains the pk and sk returns that entry",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "steve"),
          ("app2", "dave"),
          ("app2", "abigail"),
          ("app3", "shelly"),
          ("app3", "bruce"),
          ("app3", "gail"),
        ]);
        HT.remove(ht, "app3", "shelly");
      },
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app3";
        sk = "shelly";
        attributes = TH.createMockAttributes("Cleveland"); 
      }))
    ),
    test("remove on a HashTree with multiple different pk that contains the pk and sk removes that entry from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "steve"),
          ("app2", "dave"),
          ("app2", "abigail"),
          ("app3", "shelly"),
          ("app3", "bruce"),
          ("app3", "gail"),
        ]);
        let _ = HT.remove(ht, "app3", "shelly");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
        { pk = "app2"; sk = "dave"; attributes = mockAttributes },
        { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
        { pk = "app3"; sk = "gail"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
        { pk = "app1"; sk = "steve"; attributes = mockAttributes },
      ]))
    )
  ]
);

let deleteSuite = suite("delete",
  [
    test("delete on an empty HashTree does not modify the HashTree",
      do {
        let ht = HT.init();
        HT.delete(ht, "app1", "john");
        ht;
      },
      M.equals(HTM.testableHashTree(HT.init()))
    ),
    test("delete on a HashTree that contains the pk, but not the sk does not modify the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "shelly"),
          ("app1", "dave"),
        ]);
        HT.delete(ht, "app1", "john");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
      ]))
    ),
    test("delete on a HashTree with one entry that contains the pk and sk removes that entry from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
        ]);
        let _ = HT.delete(ht, "app1", "dave");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([]))
    ),
    test("delete on a HashTree with multiple different pk that contains the pk and sk removes that entry from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "steve"),
          ("app2", "dave"),
          ("app2", "abigail"),
          ("app3", "shelly"),
          ("app3", "bruce"),
          ("app3", "gail"),
        ]);
        let _ = HT.delete(ht, "app3", "shelly");
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
        { pk = "app2"; sk = "dave"; attributes = mockAttributes },
        { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
        { pk = "app3"; sk = "gail"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
        { pk = "app1"; sk = "steve"; attributes = mockAttributes },
      ]))
    )
  ]
);

let scanSuite = suite("scan",
  [
    test("on empty hashTree returns []",
      HT.scan(HT.init(), "app1", "b", "n"),
      M.equals(T.array<E.Entity>(HTM.testableEntity, []))
    ),
    test("on hashTree without the provided pk returns []",
      HT.scan(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n"),
      M.equals(T.array<E.Entity>(HTM.testableEntity, []))
    ),
    test("on hashTree with the pk, but no sk in the provided range returns []",
      HT.scan(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n"),
      M.equals(T.array<E.Entity>(HTM.testableEntity, []))
    ),
    test("on hashTree with the pk, returns only entities with that pk and sk in between the sk bounds, and returns those entities in sk sorted order", 
      HT.scan(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "zach"),
          ("app1", "matt"),
          ("app3", "john"),
          ("app1", "benny"),
          ("app2", "bruce"),
          ("app1", "nancy"),
          ("app1", "abigail"),
          ("app1", "gail"),
        ]), 
        "app1",
        "b",
        "n"
      ),
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        {
          pk = "app1";
          sk = "benny";
          attributes = mockAttributes;
        },
        {
          pk = "app1";
          sk = "gail";
          attributes = mockAttributes;
        },
        {
          pk = "app1";
          sk = "matt";
          attributes = mockAttributes;
        },
      ]))
    )
  ]
);

run(suite("HashTree", 
  [
    initSuite,
    putSuite,
    getSuite,
    removeSuite,
    deleteSuite,
    scanSuite,
  ]
));