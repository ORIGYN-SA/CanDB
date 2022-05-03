import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import DB "../src/SingleCanisterCanDB";
import HT "../src/HashTree";
import E "../src/Entity";
import HTM "./HashTreeMatchers";
import TH "./TestHelpers";

let { run;test;suite; } = S;

// Note: These tests are a sanity check that the developer exposed functions in CanDB match the functionality in the HashTree module

// Setup

let mockAttributes = TH.createMockAttributes("Cleveland");

// Tests

let initSuite = suite("init",
  [
    test("calls HT.init()",
      DB.init(),
      M.equals(HTM.testableHashTree(HT.init()))
    )
  ]
);

let getSuite = suite("get",
  [
    test("returns the same response as HT.get() on an empty DB",
      DB.get(
        DB.init(),
        {
          pk = "app1";
          sk = "john";
        }
      ),
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity, 
        HT.get(
          HT.init(),
          "app1",
          "john"
        ),
      ))
    ),
    test("returns the same response as HT.get() when the entity exists in the HashTree",
      DB.get(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "jack"),
          ("app1", "jill"),
          ("app1", "john"),
          ("app2", "john"),
        ]),
        {
          pk = "app1";
          sk = "john";
        }
      ),
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity, 
        HT.get(
          TH.createHashTreeWithPKSKMockEntries([
            ("app1", "jack"),
            ("app1", "jill"),
            ("app1", "john"),
            ("app2", "john"),
          ]),
          "app1",
          "john"
        )
      )),
    ),
    test("returns the entity when it exists in the HashTree",
      DB.get(
        TH.createHashTreeWithPKSKMockEntries([
          ("app1", "jack"),
          ("app1", "jill"),
          ("app1", "john"),
          ("app2", "john"),
        ]),
        {
          pk = "app1";
          sk = "john";
        }
      ),
      M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
        pk = "app1";
        sk = "john";
        attributes = mockAttributes;
      }))
    ),
  ]
);

let putSuite = suite("put",
  [
    test("modifies the db on an empty DB in the same way that HT.put() modifies the HashTree on an empty HashTree",
      do {
        let db = DB.init();
        DB.put(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Cleveland"))
            ];
          }
        );
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity, 
        do {
          let ht = HT.init();
          HT.put(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = mockAttributes;
            }
          );
          HTM.entries(ht);
        },
      ))
    ),
    test("modifies the db on an populated DB in the same way that HT.put() modifies the HashTree on an populated HashTree",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "james"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.put(
          db,
          {
            pk = "app1";
            sk = "francis";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Cleveland"))
            ];
          }
        );
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity, 
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "james"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.put(
            ht,
            {
              pk = "app1";
              sk = "francis";
              attributes = mockAttributes;
            }
          );
          HTM.entries(ht);
        }
      ))
    ),
    test("replaces an entity's attributes if already exists in the DB in the same way HT.put() replaces the entity",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "james"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.put(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Columbus"))
            ];
          }
        );
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity, 
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "james"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.put(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = TH.createMockAttributes("Columbus");
            }
          );
          HTM.entries(ht);
        }
      ))
    ),
  ]
);

let replaceSuite = suite("replace",
  [
    test("modifies the db on an empty DB in the same way that HT.replace() modifies the HashTree on an empty HashTree",
      do {
        let db = DB.init();
        let _ = DB.replace(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Cleveland"))
            ];
          }
        );
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity, 
        do {
          let ht = HT.init();
          let _ = HT.replace(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = mockAttributes;
            }
          );
          HTM.entries(ht);
        },
      ))
    ),
    test("replaces an entity in the db if exists in the same way that HT.replace() does",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "james"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        let _ = DB.replace(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Columbus"))
            ];
          }
        );
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "james"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          let _ = HT.replace(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = TH.createMockAttributes("Columbus"); 
            }
          );
          HTM.entries(ht);
        },
      ))
    ),
    test("returns null if inserting an entity that does not yet exist into the DB just like HT.replace()",
      do {
        let db = DB.init();
        DB.replace(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Cleveland"))
            ];
          }
        );
      },
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity,
        do {
          let ht = HT.init();
          HT.replace(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = mockAttributes;
            }
          );
        }
      ))
    ),
    test("returns the entity that was replaced if that entity already existed in the DB just like HT.replace()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "james"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.replace(
          db,
          {
            pk = "app1";
            sk = "john";
            attributes = [
              ("state", #text("OH")),
              ("year", #int(2020)),
              ("city", #text("Columbus"))
            ];
          }
        );
      },
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity, 
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "james"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.replace(
            ht,
            {
              pk = "app1";
              sk = "john";
              attributes = mockAttributes;
            }
          );
        },
      ))
    ),
  ]
);

let updateSuite = suite("update",
  [
    test("creates a new entry in the DB with the correct count if the DB is empty just like HT.update()",
      do {
        let db = DB.init();
        let _ = DB.update(db, { pk = "app1"; sk = "apples"; updateAttributeMapFunction = TH.incrementFunc; });
        HTM.entries(db)
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = HT.init();
          let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
          HTM.entries(ht)
        },
      ))
    ),
    test("creates a new entry in the DB with the correct count if the DB is doesn't contain the pk + sk just like HT.update()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "oranges"),
          ("app1", "walnuts"),
          ("app1", "grapes"),
          ("app2", "apples"),
        ]);
        let _ = DB.update(db, { pk = "app1"; sk = "apples"; updateAttributeMapFunction = TH.incrementFunc; });
        HTM.entries(db)
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "oranges"),
            ("app1", "walnuts"),
            ("app1", "grapes"),
            ("app2", "apples"),
          ]);
          let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
          HTM.entries(ht)
        },
      ))
    ),
    test("applies the updateAttributeMap function to an existing entry in the DB if the DB contains the pk + sk and the attribute just like HT.update()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "oranges"),
          ("app1", "walnuts"),
          ("app1", "grapes"),
          ("app2", "apples"),
        ]);
        DB.put(db, {
          pk = "app1";
          sk = "apples";
          attributes = [
            ("count", #int(10)),
            ("state", #text("NY")),
            ("year", #int(2022)),
            ("city", #text("Albany")),
          ];
        });
        let _ = DB.update(db, { pk = "app1"; sk = "apples"; updateAttributeMapFunction = TH.incrementFunc; });
        HTM.entries(db)
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "oranges"),
            ("app1", "walnuts"),
            ("app1", "grapes"),
            ("app2", "apples"),
          ]);
          HT.put(ht, {
            pk = "app1";
            sk = "apples";
            attributes = E.createAttributeMapFromKVPairs([
              ("count", #int(10)),
              ("state", #text("NY")),
              ("year", #int(2022)),
              ("city", #text("Albany")),
            ]);
          });
          let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
          HTM.entries(ht)
        },
      ))
    ),
  ]
);

let deleteSuite = suite("delete",
  [
    test("leaves an empty DB unchanged just like HT.delete()",
      do {
        let db = DB.init();
        DB.delete(DB.init(), { pk = "app1"; sk = "john"; });
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = HT.init();
          HT.delete(HT.init(), "app1", "john");
          HTM.entries(ht);
        }
      ))
    ),
    test("deletes an entity from a populated DB just like HT.delete()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "jack"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.delete(DB.init(), { pk = "app1"; sk = "john"; });
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "jack"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.delete(HT.init(), "app1", "john");
          HTM.entries(ht);
        }
      ))
    ),
  ]
);

let removeSuite = suite("remove",
  [
    test("leaves an empty DB unchanged just like HT.remove()",
      do {
        let db = DB.init();
        let _ = DB.remove(DB.init(), { pk = "app1"; sk = "john"; });
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = HT.init();
          let _ = HT.remove(HT.init(), "app1", "john");
          HTM.entries(ht);
        }
      ))
    ),
    test("removes an entity from a populated DB just like HT.remove()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "jack"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        let _ = DB.remove(DB.init(), { pk = "app1"; sk = "john"; });
        HTM.entries(db);
      },
      M.equals(T.array<E.Entity>(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "jack"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          let _ = HT.remove(HT.init(), "app1", "john");
          HTM.entries(ht);
        }
      ))
    ),
    test("returns null if the entity to remove does not exist in the DB just like HT.remove()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "jack"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.remove(DB.init(), { pk = "app1"; sk = "jill"; });
      },
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "jack"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.remove(HT.init(), "app1", "jill");
        }
      ))
    ),
    test("returns the removed entity if it exists in the DB just like HT.remove()",
      do {
        let db = TH.createHashTreeWithPKSKMockEntries([
          ("app1", "april"),
          ("app1", "jack"),
          ("app1", "john"),
          ("app2", "john"),
        ]);
        DB.remove(DB.init(), { pk = "app1"; sk = "john"; });
      },
      M.equals<?E.Entity>(T.optional(
        HTM.testableEntity,
        do {
          let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "april"),
            ("app1", "jack"),
            ("app1", "john"),
            ("app2", "john"),
          ]);
          HT.remove(HT.init(), "app1", "john");
        }
      ))
    ),
  ]
);

let scanSuite = suite("suite",
  [
    test("returns the same entities and nextKey on an empty DB as HT.scanLimit(), but in a record instead of a tuple",
      do {
        let scanResult = DB.scan(DB.init(), {
          pk = "app1";
          skLowerBound = "a";
          skUpperBound ="z";
          limit = 10;
          ascending = null;
        });
        (scanResult.entities, scanResult.nextKey)
      },
      M.equals(HTM.testableHashTreeScanLimitResult(
        HT.scanLimit(HT.init(), "app1", "a", "z", 10)
      ))
    ),
    test("returns the same entities and nextKey on an populated DB where results are in bounds and limit is reached as HT.scanLimit(), but in a record instead of a tuple",
      do {
        let scanResult = DB.scan(
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
          {
            pk = "app1";
            skLowerBound = "b";
            skUpperBound = "n";
            limit = 3;
            ascending = null;
          }
        );
        (scanResult.entities, scanResult.nextKey)
      },
      M.equals(HTM.testableHashTreeScanLimitResult(
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
        )
      ))
    ),
    test("if descending order is specified, returns the same entities and nextKey on an populated DB where results are in bounds and limit is reached as HT.scanLimitReverse(), but in a record instead of a tuple",
      do {
        let scanResult = DB.scan(
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
          {
            pk = "app1";
            skLowerBound = "b";
            skUpperBound = "n";
            limit = 3;
            ascending = ?false;
          }
        );
        (scanResult.entities, scanResult.nextKey)
      },
      M.equals(HTM.testableHashTreeScanLimitResult(
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
        )
      ))
    )
  ]
);

run(suite("CanDB",
  [
    initSuite,
    getSuite,
    putSuite,
    replaceSuite,
    updateSuite,
    deleteSuite,
    removeSuite,
    scanSuite
  ]
));