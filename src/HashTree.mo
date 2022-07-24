/// "HashTree" (single canister CanDB only) - the data structure underlying SingleCanisterCanDB. A HashTree is a stable HashMap, which contains a HashTable storing a mapping of an Primary Key to RangeTree,
/// where the RangeTree contains a mapping of an Entity's Sort Key to its Attributes

import HM "mo:stable-hash-map/FunctionalStableHashMap";
import RBT "mo:stable-rbtree/StableRBTree";
import A "mo:base/Array";
import Text "mo:base/Text";
import Nat32 "mo:base/Nat32";
import List "mo:base/List";
import E "./Entity";
import RT "./RangeTree";
import AssocList "mo:base/AssocList";

module {
  type HTKVs = AssocList.AssocList<E.PK, RT.RangeTree>;

  /// A HashTree is a HashMap with keys being each Entity's Primary Key and Values being RangeTrees containing the Sort Key and Attributes of that particular Entity
  public type HashTree = HM.StableHashMap<E.PK, RT.RangeTree>;

  /// Initializes a StableHashTree with initCapacity and table size zero
  public func init(): HashTree {
    HM.init<E.PK, RT.RangeTree>();
  };

  public func initPreSized(initCapacity: Nat): HashTree {
    HM.initPreSized<E.PK, RT.RangeTree>(initCapacity);
  };

  /* TODO: some type of count/size necessary for splitting a HashTree, as well as the underlying RangeTree(s)
   * If calculating from scratch, need to iterate through HashTree entries, and then each of the RTs
   * Also consider holding a count variable at the root HashTree, as well as at each of the RTs
   * Also possibly hold a count at the canister manager level.
  public func count(ht: HashTree): Nat {
  };
  */

  /// Gets an entity from the HashTree by pk + sk if that entity exists
  public func get(ht: HashTree, pk: E.PK, sk: E.SK): ?E.Entity {
    switch(HM.get<E.PK, RT.RangeTree>(ht, Text.equal, Text.hash, pk)) {
      case null { null };
      case (?rt) {
        switch(RT.get(rt, sk)) {
          case null { null };
          case (?attributeMap) { ?E.createEntity(pk, sk, attributeMap) }
        }
      }
    }
  };

  /// Insert/Replace an entity into the HashTree and returns nothing
  /// Mutates the underlying HashTree passed to this function
  public func put(ht: HashTree, entity: E.Entity): () {
    ignore replace(ht, entity);
  };

  /// Insert/Replace an entity into the HashTree and returns the previous value stored at
  /// the pk + sk if existed.
  /// Mutates the underlying HashTree passed to this function
  public func replace(ht: HashTree, entity: E.Entity): ?E.Entity {
    if (ht._count >= ht.table.size()) {
      ht.table := resizeTable(ht);
    };
    let h = Nat32.toNat(Text.hash(entity.pk));
    let pos = h % ht.table.size();

    let (kvs2, ov) = replaceRec(ht.table[pos], entity);
    ht.table[pos] := kvs2;
    switch(ov) {
      case null { ht._count += 1 };
      case _ {}
    };
    ov;
  };

  /// Creates or updates an entry in the HashTree based upon if the pk + sk of the entity provided exists.
  ///
  /// Applies a function that takes null if the entity's pk + sk does not exist, or the current AttributeMap of an existing entity and returns 
  /// a new AttributeMap that is used to update the attributes of the entity
  public func update(ht: HashTree, pk: E.PK, sk: E.SK, updateFunction: (?E.AttributeMap) -> E.AttributeMap): ?E.Entity {
    if (ht._count >= ht.table.size()) {
      ht.table := resizeTable(ht);
    };
    let h = Nat32.toNat(Text.hash(pk));
    let pos = h % ht.table.size();
    let (kvs2, ov) = updateRec(ht.table[pos], pk, sk, updateFunction);
    ht.table[pos] := kvs2;
    switch(ov) {
      case null { 
        ht._count += 1;
        null
      };
      case (?ovAttrbuteMap) {
        ?{
          pk = pk;
          sk = sk;
          attributes = ovAttrbuteMap;
        }
      }
    };
  }; 

  /// Deletes an entity from the HashTree by pk/sk if that entity exists. Does not return any value
  /// Mutates the underlying HashTree passed to this function
  public func delete(ht: HashTree, pk: E.PK, sk: E.SK): () {
    ignore remove(ht, pk, sk);
  };

  /// Deletes an entity from the HashTree by pk/sk if that entity exists, and returns the original value if that entity exists
  /// Mutates the underlying HashTree passed to this function
  public func remove(ht: HashTree, pk: E.PK, sk: E.SK): ?E.Entity {
    let m = ht.table.size();
    if (m > 0) {
      let h = Nat32.toNat(Text.hash(pk));
      let pos = h % m;
      let (kvs2, ov) = removeRec(ht.table[pos], pk, sk);
      ht.table[pos] := kvs2;

      ov;
    } else {
      null
    }
  };

  /// Returns a range of entities that exactly match the provided pk, and are in between the sk lower and upper bounds (inclusive)
  public func scan(ht: HashTree, pk: E.PK, skLowerBound: E.SK, skUpperBound: E.SK): [E.Entity] {
    switch(HM.get<E.PK, RT.RangeTree>(ht, Text.equal, Text.hash, pk)) {
      case null { [] };
      case (?rt) {
        A.map<(E.SK, E.AttributeMap), E.Entity>(RT.scan(rt, skLowerBound, skUpperBound), func((sk, attributeMap)) {
          E.createEntity(pk, sk, attributeMap)
        })
      }
    }
  };

  /// Returns a limit specified range of entities in ascending order that exactly match the provided pk, and are in between the sk lower and upper bounds (inclusive)
  public func scanLimit(ht: HashTree, pk: E.PK, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat): ([E.Entity], ?E.SK) {
    scanLimitDirection(ht, pk, skLowerBound, skUpperBound, limit, #fwd);
  };

  /// Returns a limit specified range of entities in descending order that exactly match the provided pk, and are in between the sk lower and upper bounds (inclusive)
  public func scanLimitReverse(ht: HashTree, pk: E.PK, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat): ([E.Entity], ?E.SK) {
    scanLimitDirection(ht, pk, skLowerBound, skUpperBound, limit, #bwd);
  };

  // TODO: Think about the case where deleting an entity from a range tree leaves an empty range tree
  // (i.e. should this RT be deleted from the al, and if the al is now empty, should the hash table be removed and the size decremented?)
  /// Return a boolean indicating if the two HashTrees are equivalent, ignoring deleted entries in the underlying RangeTree Red-Black Tree
  public func equal(ht1: HashTree, ht2: HashTree): Bool {
    func hashTreeEntryEqual((pk1: E.PK, rt1: RT.RangeTree), (pk2: E.PK, rt2: RT.RangeTree)): Bool {
      Text.equal(pk1, pk2) and RT.equal(rt1, rt2);
    };

    if (HM.size<E.PK, RT.RangeTree>(ht1) != HM.size<E.PK, RT.RangeTree>(ht2)) { 
      return false
    };
    var i = 0;
    while (i < HM.size<E.PK, RT.RangeTree>(ht1)) {
      if (not List.equal<(E.PK, RT.RangeTree)>(ht1.table[i], ht2.table[i], hashTreeEntryEqual)) {
        return false;
      };
      i += 1;
    }; 
    return true;
  };

  // insert the entity into the hashtable at the hashed position
  // if text of the pk exists, replaces it
  // if there is a hash collection, inserts itself by appending the entity to the list at that hash value
  // if there is nothing at the hash table position, inserts the entity 
  func replaceRec(al: HTKVs, entity: E.Entity): (HTKVs, ?E.Entity) {
    switch (al) {
      // key does not exist at hash table or collision list position
      case null {
        (?((entity.pk, RT.put(RT.init(), entity)), null), null);
      };
      // a key (matching or collision exists at hash table position)
      case (?((pk, rangeTree), tl)) {
        // key exists, replace at the RangeTree level
        if (Text.equal(pk, entity.pk)) {
          switch(RT.replace(rangeTree, entity)) {
            case (null, newRT) {
              (?((pk, newRT), tl), null)
            };
            case (?ovAttrMap, newRT) {
              (?((pk, newRT), tl), ?{
                pk = pk;
                sk = entity.sk;
                attributes = ovAttrMap;
              })
            }
          }
        // key does not match (collision), recurse on next element of collision list
        } else {
          let (nt, ov) = replaceRec(tl, entity);
          (?((pk, rangeTree), nt), ov);
        }
      } 
    }
  }; 

  func updateRec(al: HTKVs, pk: E.PK, sk: E.SK, updateFn: (?E.AttributeMap) -> E.AttributeMap): (HTKVs, ?E.AttributeMap) {
    switch (al) {
      // key does not exist at hash table or collision list position
      case null {
        let (ov, rt) = RT.update(RT.init(), sk, updateFn); //), null), null);
        (?((pk, rt), null), ov);
      };
      // a key (matching or collision exists at hash table position)
      case (?((existingPk, rangeTree), tl)) {
        // key exists, replace at the RangeTree level
        if (Text.equal(existingPk, pk)) {
          let (ovAttrbuteMap, newRT) = RT.update(rangeTree, sk, updateFn);
          (?((pk, newRT), tl), ovAttrbuteMap);
        // key does not match (collision), recurse on next element of collision list
        } else {
          let (nt, ov) = updateRec(tl, pk, sk, updateFn);
          (?((pk, rangeTree), nt), ov);
        }
      } 
    }
  }; 

  func removeRec(al: HTKVs, pkRemove: E.PK, skRemove: E.SK): (HTKVs, ?E.Entity) {
    switch (al) {
      // key does not exist at hash table or collision list position
      case null {
        (null, null);
      };
      // a key (matching or collision exists at hash table position)
      case (?((pk, rangeTree), tl)) {
        // pk exists, replace at the RangeTree level
        if (Text.equal(pk, pkRemove)) {
          switch(RT.remove(rangeTree, skRemove)) {
            // sk does not exist, remove nothing
            case (null, _) {
              (al, null)
            };
            case (?ovAttrMap, newRT) {
              (?((pk, newRT), tl), ?{
                pk = pkRemove;
                sk = skRemove;
                attributes = ovAttrMap;
              })
            }
          }
        // key does not match (collision), recurse on next element of collision list
        } else {
          let (nt, ov) = removeRec(tl, pkRemove, skRemove);
          (?((pk, rangeTree), nt), ov);
        }
      } 
    }
  };

  func resizeTable(ht: HashTree): [var HTKVs] {
    let size = 
      if (ht._count == 0) {
        if (ht.initCapacity > 0) {
          ht.initCapacity
        } else {
          1
        }
      }
      else {
        ht.table.size() * 2;
      };
    let table2 = A.init<HTKVs>(size, null); 
    for (i in ht.table.keys()) {
      var kvs = ht.table[i];
      label moveKeyVals: ()
      loop {
        switch kvs {
          case null { break moveKeyVals };
          case (?((k, v), kvsTail)) {
            let h = Nat32.toNat(Text.hash(k));
            let pos2 = h % table2.size();
            table2[pos2] := ?((k,v), table2[pos2]);
            kvs := kvsTail;
          };
        }
      };
    };
    table2;
  };

  func scanLimitDirection(ht: HashTree, pk: E.PK, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat, dir: { #fwd; #bwd }): ([E.Entity], ?E.SK) {
    switch((dir, HM.get<E.PK, RT.RangeTree>(ht, Text.equal, Text.hash, pk))) {
      case (_, null) { ([], null) };
      case (#fwd, ?rt) {
        let (results, nextKey) = RT.scanLimit(rt, skLowerBound, skUpperBound, limit);
        (
          A.map<(E.SK, E.AttributeMap), E.Entity>(results, func((sk, attributeMap)) {
            E.createEntity(pk, sk, attributeMap)
          }),
          nextKey
        )
      };
      case (#bwd, ?rt) {
        let (results, nextKey) = RT.scanLimitReverse(rt, skLowerBound, skUpperBound, limit);
        (
          A.map<(E.SK, E.AttributeMap), E.Entity>(results, func((sk, attributeMap)) {
            E.createEntity(pk, sk, attributeMap)
          }),
          nextKey
        )
      }
    }
  };
}