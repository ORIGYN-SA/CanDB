import Debug "mo:base/Debug";
import Option "mo:base/Option";

import HM "mo:stable-hash-map/FunctionalStableHashMap";
import {test; suite} "mo:test";

import E "../src/Entity";
import HT "../src/HashTree";
import HTM "./hashtree.testable";
import M "Matchers";
import RT "../src/RangeTree";
import T "./Testable";
import TH "./test.helpers";

let mockAttributes = TH.createMockAttributes("Cleveland");


suite("init", func() {
    test("initializes a hashmap of type E.PK, RangeTree", func() {
        let t1 = HT.init();
        let t2 = M.equals(HTM.testableHashTree(
            HM.init<E.PK, RT.RangeTree>()
        ));

        M.assertThat(t1, t2)
    });
    test("has a size of 0", func() {
        let t1 = HM.size<E.PK, RT.RangeTree>(HT.init());
        let t2 = M.equals(T.nat(0));
        
        M.assertThat(t1, t2)
    });
});

suite("put", func() {
    test("inserts an item into an empty HashTree", func() {

        let t1 = do {
            let ht = HT.init();
            HT.put(ht, {
                pk = "app1";
                sk = "john";
                attributes = mockAttributes; 
            });
            HTM.entries(ht)
        };
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "john"; attributes = mockAttributes }
        ]));
        
        M.assertThat(t1, t2)
       
    });
    test("inserts items with different pks into a HashTree", func() {
        let t1 = do {
            let t1 = HTM.entries(
                TH.createHashTreeWithPKSKMockEntries([
                    ("app1", "john"),
                    ("app2", "dave"),
                    ("app3", "shelly"),
                ]));
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("inserts items with different pks and multiple sks per pk into a HashTree, and the items entries are grouped in sk order by pk", func() {
        let t1 = do{
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
            ]));            
        };

        let t2 =  M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "clara"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "steve"; attributes = mockAttributes },
            { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app3"; sk = "gail"; attributes = mockAttributes },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app4"; sk = "shawn"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("replaces an entity if it already exists at the root of an sk's RangeTree", func() {
        let t1 =  do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
                ("app1", "john"),
                ("app2", "dave"),
                ("app3", "shelly"),
            ]);
            HT.put(ht, { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") });
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("replaces an entity if it exists deep in a sk's RangeTree", func() {
        let t1 = do {
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
            HTM.entries(ht)
            
        };
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") },
            { pk = "app1"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app1"; sk = "dave"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") },
        ]));

        M.assertThat(t1, t2)
    });
});

suite("get", func() {
    test("returns null if used on an empty HashTree", func(){
        let t1 = do {
           let entity = HT.get(HT.init(), "app1", "john");
           T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)
      
    });
    test("returns null if the pk does not exist", func() {
        let t1 = do {
           let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app2", "shelly"),
            ("app3", "dave"),
            ]);
           let entity = HT.get(ht, "app1", "john");
           T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)
      
    });
    test("returns null if the sk does not exist", func(){
        let t1 = do {
           let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app2", "shelly"),
            ("app3", "dave"),
           ]);
           let entity = HT.get(ht, "app1", "john");
           T.optional<E.Entity>(HTM.testableEntity, entity);           
        };

        let t2 = M.isNull<E.Entity>();
        
        M.assertThat(t1, t2)
      
    });
    test("returns the entity if it exists and is the only item in the hashtree", func() {
        
        let t1 = do {
           let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "dave"),
            ]);
            HT.get(ht, "app1", "dave")
        };

        let t2 = M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
            pk = "app1";
            sk = "dave";
            attributes = TH.createMockAttributes("Cleveland")
        }));

        M.assertThat(t1, t2)
      
    });
    test("returns the entity if it exists in a range tree", func() {
        let t1 =  do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "dave"),
            ("app1", "john"),
            ("app1", "barry"),
            ("app1", "alice"),
            ("app2", "barry"),
            ]);
            HT.get(ht, "app1", "barry")
            
        };

        let t2 = M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
            pk = "app1";
            sk = "barry";
            attributes = TH.createMockAttributes("Cleveland")
        }));

        M.assertThat(t1, t2)
         
    });
    
});

suite("replace", func() {
    test("inserts an item into an empty HashTree", func() {
        let t1 = do {
            let ht = HT.init();
            let _ = HT.replace(ht, {
                pk = "app1";
                sk = "john";
                attributes = mockAttributes; 
            });
            HTM.entries(ht)            
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "john"; attributes = mockAttributes }
        ]));

        M.assertThat(t1, t2)

    });
    test("returns null when an item does not exist in the HashTree", func() {
        let t1 = do {
            let ht = HT.init();
            T.optional(
                HTM.testableEntity,
                HT.replace(ht, {
                    pk = "app1";
                    sk = "john";
                    attributes = mockAttributes; 
                })
            );            
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)

    });
    test("inserts items with different pks into a HashTree", func() {
        let t1 = do {
            let ht = HT.init();
            var res = HT.replace(ht, { pk = "app1"; sk = "john"; attributes = mockAttributes; });
            res := HT.replace(ht, { pk = "app2"; sk = "dave"; attributes = mockAttributes; });
            res := HT.replace(ht, { pk = "app3"; sk = "shelly"; attributes = mockAttributes; });
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("inserts items with different pks and multiple sks per pk into a HashTree, and the items entries are grouped in sk order by pk", func() {

        let t1 =  do {
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
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "clara"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "steve"; attributes = mockAttributes },
            { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app3"; sk = "gail"; attributes = mockAttributes },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app4"; sk = "shawn"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("replaces an entity if it already exists at the root of an sk's RangeTree", func() {

        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "john"),
            ("app2", "dave"),
            ("app3", "shelly"),
            ]);
            var res = HT.replace(ht, { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") });
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app2"; sk = "dave"; attributes = TH.createMockAttributes("Columbus") },
            { pk = "app3"; sk = "shelly"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("replaces an entity if it exists deep in a sk's RangeTree", func() {
        let t1 = do {
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
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") },
            { pk = "app1"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app1"; sk = "dave"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "shelly"; attributes = TH.createMockAttributes("Akron") },
        ]));

        M.assertThat(t1, t2)
        
    });
    test("returns the old entity if the entity existed in the HashTree and was replaced", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "john"),
            ("app1", "dave"),
            ("app1", "shelly"),
            ("app1", "alice"),
            ("app1", "bruce"),
            ("app1", "abigail"),
            ]);
            
            HT.replace(ht, { pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Columbus") })

        };

        let t2 = M.equals(T.optional(
            HTM.testableEntity,
            ?{ pk = "app1"; sk = "alice"; attributes = TH.createMockAttributes("Cleveland") },
        ));

        M.assertThat(t1, t2)
         
    });
});

suite("update", func() {
    test("returns a null entity if the HashTree is empty", func() {
        
        let t1 = do {
            let entity = HT.update(HT.init(), "app1", "apples", TH.incrementFunc);
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)

    });
    test("creates a new entity in the HashTree with the correct count if the HashTree is empty", func() {
        let t1 = do {
            let ht = HT.init();
            let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
            HTM.entries(ht)
        };

        let t2 = M.equals(HTM.testableHashTreeEntries([
            { 
            pk = "app1"; 
            sk = "apples"; 
            attributes = E.createAttributeMapFromKVPairs([
                ("count", #int(1))
            ])
            }
        ]));

        M.assertThat(t1, t2)
    });
    test("returns a null entity if the HashTree does not contain the pk", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app2", "apples"),
            ("app2", "oranges"),
            ("app3", "apples"),
            ]);
            let entity = HT.update(ht, "app1", "apples", TH.incrementFunc);
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)
    });
    test("creates a new entity in the HashTree with the correct count if the HashTree does not contain the pk", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app2", "apples"),
            ("app2", "oranges"),
            ("app3", "apples"),
            ]);
            let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
            HTM.entries(ht)
        };

        let t2 = M.equals(HTM.testableHashTreeEntries([
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
        ]));

        M.assertThat(t1, t2)


    });
    test("returns a null entity if the HashTree does not contain the pk + sk", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app2", "apples"),
            ("app2", "oranges"),
            ("app3", "apples"),
            ]);
            let entity = HT.update(ht, "app1", "apples", TH.incrementFunc);
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();
        
        M.assertThat(t1, t2)
            
    });
    test("creates a new entity in the HashTree with the correct count if the HashTree does not contain the pk + sk", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "oranges"),
            ("app2", "apples"),
            ("app2", "oranges"),
            ]);
            let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
            HTM.entries(ht);
        };

        let t2 = M.equals(HTM.testableHashTreeEntries([
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
        ]));
            
        M.assertThat(t1, t2)

    });
    test("returns the old entity prior to the update if it existed the HT", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "apples"),
            ("app1", "oranges"),
            ("app2", "apples"),
            ("app2", "oranges"),
            ]);
            HT.update(ht, "app1", "apples", TH.incrementFunc)
        };

        let t2 =  M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
            pk = "app1";
            sk = "apples";
            attributes = mockAttributes
        }));
            
        M.assertThat(t1, t2)
        
    });
    test("correctly adds the count attribute to an entity in the HashTree if it existed in the HT but the count attribute did not yet exist", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "apples"),
            ("app1", "oranges"),
            ("app2", "apples"),
            ("app2", "oranges"),
            ]);
            let _ = HT.update(ht, "app1", "apples", TH.incrementFunc);
            HTM.entries(ht)
        };

        let t2 = M.equals(HTM.testableHashTreeEntries([
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
        ]));

        M.assertThat(t1, t2)
        
    });
    test("correctly updates the count attribute for an entity in the HashTree if it existed in the HT and the count attribute did exist", func() {
        assert  do {
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
            let t1 = HTM.entries(ht);
           
            let t2 = HTM.testableHashTreeEntries([
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
            ]);
            
            t1 == t2
        }
    });
});

suite("remove", func() {
    test("remove on an empty HashTree returns null", func() {
        let t1 = do {
            let ht = HT.init();
            let entity = HT.remove(ht, "app1", "john");
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)
    });
    test("remove on an empty HashTree does not modify the HashTree", func() {
        let t1 =  do {
            let ht = HT.init();
            let entity = HT.remove(ht, "app1", "john");
            ht
        };

        let t2 = M.equals(HTM.testableHashTree(HT.init()));

        M.assertThat(t1, t2)
    });
    test("remove on a HashTree that does not contain the pk returns null", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "shelly"),
            ("app1", "dave"),
            ]);
            let entity = HT.remove(ht, "app2", "shelly");
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 =  M.isNull<E.Entity>();
    });
    test("remove on a HashTree that contains the pk, but not the sk returns null", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "shelly"),
            ("app1", "dave"),
            ]);
            let entity = HT.remove(ht, "app1", "john");
            T.optional<E.Entity>(HTM.testableEntity, entity)
        };

        let t2 = M.isNull<E.Entity>();

        M.assertThat(t1, t2)

    });
    test("remove on a HashTree that contains the pk and sk returns the removed entity", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "shelly"),
            ("app1", "dave"),
            ]);
            
            HT.remove(ht, "app1", "dave")
        };

        let t2 =M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
            pk = "app1";
            sk = "dave";
            attributes = TH.createMockAttributes("Cleveland");
        }));

        M.assertThat(t1, t2)

    });
    test("remove on a HashTree that contains the pk, but not the sk does not modify the HashTree", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "shelly"),
            ("app1", "dave"),
            ]);
            let entity = HT.remove(ht, "app1", "john");
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "dave"; attributes = mockAttributes },
            { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("remove on a HashTree with one entity that contains the pk and sk removes that entity from the HashTree", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "dave"),
            ]);
            let _ = HT.remove(ht, "app1", "dave");
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, []));

        M.assertThat(t1, t2)

    });
    test("remove on a HashTree with multiple different pk that contains the pk and sk returns that entity", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "john"),
            ("app1", "steve"),
            ("app2", "dave"),
            ("app2", "abigail"),
            ("app3", "shelly"),
            ("app3", "bruce"),
            ("app3", "gail"),
            ]);
            
            HT.remove(ht, "app3", "shelly")
        };

        let t2 = M.equals<?E.Entity>(T.optional(HTM.testableEntity, ?{
            pk = "app3";
            sk = "shelly";
            attributes = TH.createMockAttributes("Cleveland"); 
        }));

        M.assertThat(t1, t2)
        
    });
    test("remove on a HashTree with multiple different pk that contains the pk and sk removes that entity from the HashTree", func() {
        let t1 =  do {
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
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity,[
            { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app3"; sk = "gail"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "steve"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    
});

suite("delete", func() {
    test("delete on an empty HashTree does not modify the HashTree", func() {
        let t1 = do {
            let ht = HT.init();
            HT.delete(ht, "app1", "john");
            ht
        };
             
        let t2 = M.equals(HTM.testableHashTree(HT.init()));

        M.assertThat(t1, t2)

    });
    test("delete on a HashTree that contains the pk, but not the sk does not modify the HashTree", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "shelly"),
            ("app1", "dave"),
            ]);
            HT.delete(ht, "app1", "john");
            HTM.entries(ht)
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity,[
            { pk = "app1"; sk = "dave"; attributes = mockAttributes },
            { pk = "app1"; sk = "shelly"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
    test("delete on a HashTree with one entity that contains the pk and sk removes that entity from the HashTree", func() {
        let t1 = do {
            let ht = TH.createHashTreeWithPKSKMockEntries([
            ("app1", "dave"),
            ]);
            let _ = HT.delete(ht, "app1", "dave");
            HTM.entries(ht)
        };
        
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity,[]));

        M.assertThat(t1, t2)
    });
    test("delete on a HashTree with multiple different pk that contains the pk and sk removes that entity from the HashTree", func() {
        let t1 = do {
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
            HTM.entries(ht)
          
        };

        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity,[
            { pk = "app2"; sk = "abigail"; attributes = mockAttributes },
            { pk = "app2"; sk = "dave"; attributes = mockAttributes },
            { pk = "app3"; sk = "bruce"; attributes = mockAttributes },
            { pk = "app3"; sk = "gail"; attributes = mockAttributes },
            { pk = "app1"; sk = "john"; attributes = mockAttributes },
            { pk = "app1"; sk = "steve"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
});

suite("scan", func() {
    test("on empty hashTree returns []", func() {
        let t1 = HT.scan(HT.init(), "app1", "b", "n");
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, []));

        M.assertThat(t1, t2)
    });
    test("on hashTree without the provided pk returns []", func() {
        let t1 = HT.scan(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n");
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, []));

         M.assertThat(t1, t2)
    });
    test("on hashTree with the pk, but no sk in the provided range returns []", func() {
        let t1 = HT.scan(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n");
        let t2 = M.equals(T.array<E.Entity>(HTM.testableEntity, []));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk, returns only entities with that pk and sk in between the sk bounds, and returns those entities in sk sorted order", func() {
        let t1 = HT.scan(TH.createHashTreeWithPKSKMockEntries([
            ("app1", "zach"),
            ("app1", "matt"),
            ("app3", "john"),
            ("app1", "benny"),
            ("app2", "bruce"),
            ("app1", "nancy"),
            ("app1", "abigail"),
            ("app1", "gail"),
            ]), "app1", "b", "n");
        let t2 =  M.equals(T.array<E.Entity>(HTM.testableEntity, [
            { pk = "app1"; sk = "benny"; attributes = mockAttributes },
            { pk = "app1"; sk = "gail"; attributes = mockAttributes },
            { pk = "app1"; sk = "matt"; attributes = mockAttributes },
        ]));

        M.assertThat(t1, t2)
    });
});

suite("scanLimit", func() {
    test("on empty hashTree the result returned is [], and null nextKey", func() {
        let t1 = HT.scanLimit(HT.init(), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree without the provided pk returns [] and null nextKey", func() {
        let t1 = HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk, but no sk in the provided range returns [] and null nextKey", func() {
        let t1 = HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk and limit > result set, returns only entities with that pk and sk in between the sk bounds, and returns all those entities in the bounds in sk sorted order and null nextKey", func() {
        let t1 = HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([
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
            ]), "app1", "b", "n", 10);
        let t2 = M.equals(HTM.testableHashTreeScanLimitResult([
            { pk = "app1"; sk = "benny"; attributes = mockAttributes },
            { pk = "app1"; sk = "chris"; attributes = mockAttributes },
            { pk = "app1"; sk = "gail"; attributes = mockAttributes },
            { pk = "app1"; sk = "logan"; attributes = mockAttributes },
            { pk = "app1"; sk = "matt"; attributes = mockAttributes },
        ], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk and limit < result set, returns only entities with that pk and sk in between the sk bounds, and returns the # of entities in the bounds according to the limit specified and in sk sorted order and the appropriate nextKey", func() {
        let t1 = HT.scanLimit(TH.createHashTreeWithPKSKMockEntries([
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
            ]), "app1", "b", "n", 3);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([
            { pk = "app1"; sk = "benny"; attributes = mockAttributes },
            { pk = "app1"; sk = "chris"; attributes = mockAttributes },
            { pk = "app1"; sk = "gail"; attributes = mockAttributes },
        ], ?"logan"));

        M.assertThat(t1, t2)
    });
});

suite("scanLimitReverse", func() {
    test("on empty hashTree returns [] and null nextKey", func() {
        let t1 = HT.scanLimitReverse(HT.init(), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree without the provided pk returns [] and null nextKey", func() {
        let t1 = HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([("app2", "john")]), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk, but no sk in the provided range returns [] and null nextKey", func() {
        let t1 = HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([("app1", "abigail")]), "app1", "b", "n", 5);
        let t2 =  M.equals(HTM.testableHashTreeScanLimitResult([], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk and limit > result set, returns only entities with that pk and sk in between the sk bounds, and returns all those entities in the bounds in sk sorted order and null nextKey", func() {
        let t1 = HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([
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
            ]), "app1", "b", "n", 10);
        let t2 = M.equals(HTM.testableHashTreeScanLimitResult([
            { pk = "app1"; sk = "matt"; attributes = mockAttributes },
            { pk = "app1"; sk = "logan"; attributes = mockAttributes },
            { pk = "app1"; sk = "gail"; attributes = mockAttributes },
            { pk = "app1"; sk = "chris"; attributes = mockAttributes },
            { pk = "app1"; sk = "benny"; attributes = mockAttributes },
        ], null));

        M.assertThat(t1, t2)
    });
    test("on hashTree with the pk and limit < result set, returns only entities with that pk and sk in between the sk bounds, and returns the # of entities in the bounds according to the limit specified and in sk sorted order and the appropriate nextKey", func() {
        let t1 = HT.scanLimitReverse(TH.createHashTreeWithPKSKMockEntries([
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
            ]), "app1", "b", "n", 3);
        let t2 = M.equals(HTM.testableHashTreeScanLimitResult([
            { pk = "app1"; sk = "matt"; attributes = mockAttributes },
            { pk = "app1"; sk = "logan"; attributes = mockAttributes },
            { pk = "app1"; sk = "gail"; attributes = mockAttributes },
        ], ?"chris"));

        M.assertThat(t1, t2)
    });
});