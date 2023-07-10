import HM "mo:stable-hash-map/FunctionalStableHashMap";
import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";

import E "../src/Entity";
import HT "../src/HashTree";
import HTM "./HashTreeMatchers";
import RT "../src/RangeTree";
import TH "./TestHelpers";

// Setup

let { run;test;suite; } = S;

let mockAttributes = TH.createMockAttributes("Cleveland");

// Tests

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
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
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
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
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
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
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
    test("replaces an entity if it already exists at the root of an sk's RangeTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app2", "dave"),
          ("app3", "shelly"),
        ]);
        HT.put(ht, { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
      ]))
    ),
    test("replaces an entity if it exists deep in a sk's RangeTree",
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
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
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

let replaceSuite = suite("replace",
  [
    test("inserts an item into an empty HashTree",
      do {
        let ht = HT.init();
        let _ = HT.replace(ht, {
          pk = "app1";
          sk = "john";
          attributes = mockAttributes; 
        });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app1"; sk = "john"; attributes = mockAttributes }
      ]))
    ),
    test("returns null when an item does not exist in the HashTree",
      do {
        let ht = HT.init();
        T.optional(
          HTM.testableEntity,
          HT.replace(ht, {
            pk = "app1";
            sk = "john";
            attributes = mockAttributes; 
          })
        )
      },
      M.isNull<E.Entity>()
    ),
    test("inserts items with different pks into a HashTree",
      do {
        let ht = HT.init();
        var res = HT.replace(ht, { pk = "app1"; sk = "john"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app2"; sk = "dave"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app3"; sk = "shelly"; attributes = mockAttributes; });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app2"; sk = "dave"; attributes = mockAttributes },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
      ]))
    ),
    test("inserts items with different pks and multiple sks per pk into a HashTree, and the items entries are grouped in sk order by pk",
      do {
        let ht = HT.init();
        var res = HT.replace(ht, { pk = "app1"; sk = "john"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app1"; sk = "steve"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app1"; sk = "clara"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app2"; sk = "dave"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app2"; sk = "abigail"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app3"; sk = "shelly"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app3"; sk = "bruce"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app3"; sk = "gail"; attributes = mockAttributes; });
        res := HT.replace(ht, { pk = "app4"; sk = "shawn"; attributes = mockAttributes; });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
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
    test("replaces an entity if it already exists at the root of an sk's RangeTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app2", "dave"),
          ("app3", "shelly"),
        ]);
        var res = HT.replace(ht, { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") },
        { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
      ]))
    ),
    test("replaces an entity if it exists deep in a sk's RangeTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "dave"),
          ("app1", "shelly"),
          ("app1", "alice"),
          ("app1", "bruce"),
          ("app1", "abigail"),
        ]);
        var res = HT.replace(ht, { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") });
        res := HT.replace(ht, { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") });
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app1"; sk = "abigail"; attributes = mockAttributes },
        { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") },
        { pk = "app1"; sk = "bruce"; attributes = mockAttributes },
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "john"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") },
      ]))
    ),
    test("returns the old entity if the entity existed in the HashTree and was replaced",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "john"),
          ("app1", "dave"),
          ("app1", "shelly"),
          ("app1", "alice"),
          ("app1", "bruce"),
          ("app1", "abigail"),
        ]);
        HT.replace(ht, { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") });
      },
      M.equals(T.optional(
        HTM.testableEntity,
        ?{ pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Cleveland") },
      ))
    ),
  ]
);

let updateSuite = suite("update",
  [
    test("returns a null entity if the HashTree is empty",
      do {
        let entity = HT.update(HT.init(), "app1", "apples", TH.incrementFunc);
        T.optional<E.Entity>(HTM.testableEntity, entity)
      },
      M.isNull<E.Entity>()
    ),
    test("creates a new entity in the HashTree with the correct count if the HashTree is empty",
      do {
        let ht = HT.init();
        let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { 
          pk = "app1"; 
          sk = "apples"; 
          attributes = E.createAttributeMapFromKVPairs([
            ("count", #int(1))
          ])
        }
      ]))
    ),
    test("returns a null entity if the HashTree does not contain the pk",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app2", "apples"),
          ("app2", "oranges"),
          ("app3", "apples"),
        ]);
        let entity = HT.update(ht, "app1", "apples", TH.incrementFunc);
        T.optional<E.Entity>(HTM.testableEntity, entity)
      },
      M.isNull<E.Entity>()
    ),
    test("creates a new entity in the HashTree with the correct count if the HashTree does not contain the pk",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app2", "apples"),
          ("app2", "oranges"),
          ("app3", "apples"),
        ]);
        let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "apples"; attributes = mockAttributes },
        { pk = "app2"; sk = "oranges"; attributes = mockAttributes },
        { pk = "app3"; sk = "apples"; attributes = mockAttributes },
        { 
          pk = "app1"; 
          sk = "apples"; 
          attributes = E.createAttributeMapFromKVPairs([
            ("count", #int(1)),
          ])
        },
      ]))
    ),
    test("returns a null entity if the HashTree does not contain the pk + sk",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "oranges"),
          ("app2", "apples"),
          ("app2", "oranges"),
        ]);
        let entity = HT.update(ht, "app1", "apples", TH.incrementFunc);
        T.optional<E.Entity>(HTM.testableEntity, entity)
      },
      M.isNull<E.Entity>()
    ),
    test("creates a new entity in the HashTree with the correct count if the HashTree does not contain the pk + sk",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "oranges"),
          ("app2", "apples"),
          ("app2", "oranges"),
        ]);
        let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "apples"; attributes = mockAttributes },
        { pk = "app2"; sk = "oranges"; attributes = mockAttributes },
        { 
          pk = "app1"; 
          sk = "apples"; 
          attributes = E.createAttributeMapFromKVPairs([
            ("count", #int(1))
          ])
        },
        { pk = "app1"; sk = "oranges"; attributes = mockAttributes },
      ]))
    ),
    test("returns the old entity prior to the update if it existed the HT",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "apples"),
          ("app1", "oranges"),
          ("app2", "apples"),
          ("app2", "oranges"),
        ]);
        HT.update(ht, "app1", "apples", TH.incrementFunc);
      },
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app1";
        sk = "apples";
        attributes = mockAttributes
      }))
    ),
    test("correctly adds the count attribute to an entity in the HashTree if it existed in the HT but the count attribute did not yet exist",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "apples"),
          ("app1", "oranges"),
          ("app2", "apples"),
          ("app2", "oranges"),
        ]);
        let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "apples"; attributes = mockAttributes },
        { pk = "app2"; sk = "oranges"; attributes = mockAttributes },
        { 
          pk = "app1"; 
          sk = "apples"; 
          attributes = E.createAttributeMapFromKVPairs([
            ("state", #text("OH")),
            ("year", #int(2020)),
            ("city", #text("Cleveland")),
            ("count", #int(1)),
            ("isCountNull", #bool(false))
          ])
        },
        { pk = "app1"; sk = "oranges"; attributes = mockAttributes },
      ]))
    ),
    test("correctly updates the count attribute for an entity in the HashTree if it existed in the HT and the count attribute did exist",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "oranges"),
          ("app2", "apples"),
          ("app2", "oranges"),
        ]);
        HT.put(ht, {
          pk = "app1";
          sk = "apples";
          attributes = E.createAttributeMapFromKVPairs([
            ("state", #text("CA")),
            ("year", #int(2021)),
            ("city", #text("Pasadena")),
            ("count", #int(21)),
          ]);
        });
        let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
        HTM.entries(ht);
      },
      M.equals(HTM.testableHashTreeEntries([
        { pk = "app2"; sk = "apples"; attributes = mockAttributes },
        { pk = "app2"; sk = "oranges"; attributes = mockAttributes },
        { 
          pk = "app1"; 
          sk = "apples"; 
          attributes = E.createAttributeMapFromKVPairs([
            ("state", #text("CA")),
            ("year", #int(2021)),
            ("city", #text("Pasadena")),
            ("count", #int(22)),
            ("isCountNull", #bool(false))
          ])
        },
        { pk = "app1"; sk = "oranges"; attributes = mockAttributes },
      ]))
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
    test("remove on a HashTree that contains the pk and sk returns the removed entity",
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
      M.equals(T.array<E.Entity>(HTM.testableEntity, [
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
      ]))
    ),
    test("remove on a HashTree with one entity that contains the pk and sk removes that entity from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
        ]);
        let _ = HT.remove(ht, "app1", "dave");
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity, []))
    ),
    test("remove on a HashTree with multiple different pk that contains the pk and sk returns that entity",
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
    test("remove on a HashTree with multiple different pk that contains the pk and sk removes that entity from the HashTree",
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
      M.equals(T.array<E.Entity>(HTM.testableEntity,[
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
      M.equals(T.array<E.Entity>(HTM.testableEntity,[
        { pk = "app1"; sk = "dave"; attributes = mockAttributes },
        { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
      ]))
    ),
    test("delete on a HashTree with one entity that contains the pk and sk removes that entity from the HashTree",
      do {
        let ht = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "dave"),
        ]);
        let _ = HT.delete(ht, "app1", "dave");
        HTM.entries(ht);
      },
      M.equals(T.array<E.Entity>(HTM.testableEntity,[]))
    ),
    test("delete on a HashTree with multiple different pk that contains the pk and sk removes that entity from the HashTree",
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
      M.equals(T.array<E.Entity>(HTM.testableEntity,[
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
        { pk = "app1"; sk = "benny"; attributes = mockAttributes },
        { pk = "app1"; sk = "gail"; attributes = mockAttributes },
        { pk = "app1"; sk = "matt"; attributes = mockAttributes },
      ]))
    )
  ]
);

let scanLimitSuite = suite("scanLimit",
  [
    test("on empty hashTree the result returned is [], and null nextKey",
      HT.scanLimit(HT.init(), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree without the provided pk returns [] and null nextKey",
      HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree with the pk, but no sk in the provided range returns [] and null nextKey",
      HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree with the pk and limit > result set, returns only entities with that pk and sk in between the sk bounds, and returns all those entities in the bounds in sk sorted order and null nextKey", 
      HT.scanLimit(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "zach"),
          ("app1", "matt"),
          ("app3", "john"),
          ("app1", "benny"),
          ("app2", "bruce"),
          ("app1", "chris"),
          ("app1", "nancy"),
          ("app1", "abigail"),
          ("app1", "gail"),
          ("app1", "logan"),
        ]), 
        "app1",
        "b",
        "n",
        10
      ),
      M.equals(HTM.testableHashTreeScanLimitResult([
        { pk = "app1"; sk = "benny"; attributes = mockAttributes },
        { pk = "app1"; sk = "chris"; attributes = mockAttributes },
        { pk = "app1"; sk = "gail"; attributes = mockAttributes },
        { pk = "app1"; sk = "logan"; attributes = mockAttributes },
        { pk = "app1"; sk = "matt"; attributes = mockAttributes },
      ], null))
    ),
    test("on hashTree with the pk and limit < result set, returns only entities with that pk and sk in between the sk bounds, and returns the # of entities in the bounds according to the limit specified and in sk sorted order and the appropriate nextKey", 
      HT.scanLimit(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "zach"),
          ("app1", "matt"),
          ("app3", "john"),
          ("app1", "benny"),
          ("app2", "bruce"),
          ("app1", "chris"),
          ("app1", "nancy"),
          ("app1", "abigail"),
          ("app1", "gail"),
          ("app1", "logan"),
        ]), 
        "app1",
        "b",
        "n",
        3
      ),
      M.equals(HTM.testableHashTreeScanLimitResult([
        { pk = "app1"; sk = "benny"; attributes = mockAttributes },
        { pk = "app1"; sk = "chris"; attributes = mockAttributes },
        { pk = "app1"; sk = "gail"; attributes = mockAttributes },
      ], ?"logan"))
    )
  ]
);

let scanLimitReverseSuite = suite("scanLimitReverse",
  [
    test("on empty hashTree returns [] and null nextKey",
      HT.scanLimitReverse(HT.init(), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree without the provided pk returns [] and null nextKey",
      HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree with the pk, but no sk in the provided range returns [] and null nextKey",
      HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n", 5),
      M.equals(HTM.testableHashTreeScanLimitResult([], null))
    ),
    test("on hashTree with the pk and limit > result set, returns only entities with that pk and sk in between the sk bounds, and returns all those entities in the bounds in sk sorted order and null nextKey", 
      HT.scanLimitReverse(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "zach"),
          ("app1", "matt"),
          ("app3", "john"),
          ("app1", "benny"),
          ("app2", "bruce"),
          ("app1", "chris"),
          ("app1", "nancy"),
          ("app1", "abigail"),
          ("app1", "gail"),
          ("app1", "logan"),
        ]), 
        "app1",
        "b",
        "n",
        10
      ),
      M.equals(HTM.testableHashTreeScanLimitResult([
        { pk = "app1"; sk = "matt"; attributes = mockAttributes },
        { pk = "app1"; sk = "logan"; attributes = mockAttributes },
        { pk = "app1"; sk = "gail"; attributes = mockAttributes },
        { pk = "app1"; sk = "chris"; attributes = mockAttributes },
        { pk = "app1"; sk = "benny"; attributes = mockAttributes },
      ], null))
    ),
    test("on hashTree with the pk and limit < result set, returns only entities with that pk and sk in between the sk bounds, and returns the # of entities in the bounds according to the limit specified and in sk sorted order and the appropriate nextKey", 
      HT.scanLimitReverse(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "zach"),
          ("app1", "matt"),
          ("app3", "john"),
          ("app1", "benny"),
          ("app2", "bruce"),
          ("app1", "chris"),
          ("app1", "nancy"),
          ("app1", "abigail"),
          ("app1", "gail"),
          ("app1", "logan"),
        ]), 
        "app1",
        "b",
        "n",
        3
      ),
      M.equals(HTM.testableHashTreeScanLimitResult([
        { pk = "app1"; sk = "matt"; attributes = mockAttributes },
        { pk = "app1"; sk = "logan"; attributes = mockAttributes },
        { pk = "app1"; sk = "gail"; attributes = mockAttributes },
      ], ?"chris"))
    )
  ]
);

run(suite("HashTree", 
  [
    initSuite,
    putSuite,
    replaceSuite,
    getSuite,
    updateSuite,
    removeSuite,
    deleteSuite,
    scanSuite,
    scanLimitSuite,
    scanLimitReverseSuite,
  ]
));