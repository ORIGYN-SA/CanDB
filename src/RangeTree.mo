/// "RangeTree" - a stable Red-Black Tree for storing the relationship between an Entity's Sort Key and its Attributes

import Buffer "mo:base/Buffer";
import I "mo:base/Iter";
import Int "mo:base/Int";
import List "mo:base/List";
import Option "mo:base/Option";
import Stack "mo:base/Stack";
import Text "mo:base/Text";

import RBT "mo:stable-rbtree/StableRBTree";

import E "./Entity";

module {

  /// A RangeTree data structure is a Red-Black Tree mapping of a Sort Key (Text) to an AttributeMap
  public type RangeTree = RBT.Tree<E.SK, E.AttributeMap>;

  /// Initializes an empty RangeTree
  public func init(): RangeTree {
    RBT.init<E.SK, E.AttributeMap>();
  };

  /// Returns an entry from the RangeTree based on the sk provided that sk exists in the RangeTree with a non-null AttributeMap
  public func get(rt: RangeTree, sk: E.SK): ?E.AttributeMap {
    RBT.get<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  /// Creates or replaces an entry in the RangeTree based upon if the sk of the entity provided exists. Returns the new RangeTree
  public func put(rt: RangeTree, entity: E.Entity): RangeTree {
    RBT.put<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  /// Creates or replaces an entry in the RangeTree based upon if the sk of the entity provided exists. Returns the old AttributeMap if the sk existed and the new RangeTree
  public func replace(rt: RangeTree, entity: E.Entity): (?E.AttributeMap, RangeTree) {
    RBT.replace<E.SK, E.AttributeMap>(
      rt,
      Text.compare,
      entity.sk,
      entity.attributes
    );
  };

  /// Creates or updates an entry in the RangeTree based upon if the sk of the entity provided exists.
  ///
  /// The updateFunction parameter applies a function that takes null if the entry does not exist, or the current AttributeMap of an existing entry and returns 
  /// a new AttributeMap that is used to update the attributeMap entry. 
  public func update(rt: RangeTree, sk: E.SK, updateFunction: (?E.AttributeMap) -> E.AttributeMap): (?E.AttributeMap, RangeTree) {
    updateRoot(rt, sk, updateFunction);
  };

  /// Deletes an entry from the RangeTree based upon if the sk of the entity provided exists. Returns the new RangeTree
  public func delete(rt: RangeTree, sk: E.SK): RangeTree {
    RBT.delete<E.SK, E.AttributeMap>(rt, Text.compare, sk);
  };

  /// Deletes an entry from the RangeTree based upon if the sk of the entity provided exists. Returns the deleted AttributeMap if the sk existed and the new RangeTree
  public func remove(rt: RangeTree, sk: E.SK): (?E.AttributeMap, RangeTree) {
    RBT.remove<E.SK, E.AttributeMap>(rt, Text.compare, sk)
  };

  type Direction = { #fwd; #bwd };

  /// Performs a in-order scan of the RangeTree between the provided SortKey bounds, returning a number of matching entries in ascending order limited by the limit parameter specified in an array formatted as (SK, AttributeMap) for each entry
  public func scanLimit(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat): ([(E.SK, E.AttributeMap)], ?E.SK) {
    scanLimitDirection(rt, skLowerBound, skUpperBound, limit, #fwd);
  };

  /// Performs a reverse-order scan of the RangeTree between the provided SortKey bounds, returning a number of matching entries in descending order limited by the limit parameter specified in an array formatted as (SK, AttributeMap) for each entry
  public func scanLimitReverse(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat): ([(E.SK, E.AttributeMap)], ?E.SK) {
    scanLimitDirection(rt, skLowerBound, skUpperBound, limit, #bwd);
  };

  func scanLimitDirection(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK, limit: Nat, dir: Direction): ([(E.SK, E.AttributeMap)], ?E.SK) {
    switch(Text.compare(skLowerBound, skUpperBound)) {
      // return empty array if lower bound is greater than upper bound      
      // TODO: consider returning an error in this case?
      case (#greater) { ([], null) };
      // return the single entry if exists if the lower and upper bounds are equivalent
      case (#equal) { 
        switch(get(rt, skLowerBound)) {
          case null { ([], null) };
          case (?map) { ([(skLowerBound, map)], null) }
        }
      };
      case (#less) { 
        iterScanLimit(rt, skLowerBound, skUpperBound, limit, dir)
      }
    }
  };

  // TODO: Decide if this should be public (gauge community feedback)
  /// Not recommended that this is used because it's then more likely that a developer will run into the 2MB egress limit. A limit should
  /// therefore be enforced
  /// Performs a full scan of the RangeTree between the provided Sort Key bounds, returning an array of the matching (SK, AttributeMap) for each entry
  public func scan(rt: RangeTree, skLowerBound: E.SK, skUpperBound: E.SK): [(E.SK, E.AttributeMap)] {
    switch(Text.compare(skLowerBound, skUpperBound)) {
      case (#greater) { [] };
      case (#equal) { 
        switch(get(rt, skLowerBound)) {
          case null { [] };
          case (?map) { [(skLowerBound, map)] }
        }
      };
      case (#less) { 
        I.toArray(iterScan(rt, skLowerBound, skUpperBound))
      }
    }
  }; 

  /// Returns an iterator of all entries in the RangeTree 
  public func entries(rt: RangeTree): I.Iter<(E.SK, E.AttributeMap)> {
    RBT.entries<E.SK, E.AttributeMap>(rt);
  };

  /// Performs an equality check between two RangeTrees
  public func equal(rt1: RangeTree, rt2: RangeTree): Bool {
    RBT.equalIgnoreDeleted<E.SK, E.AttributeMap>(rt1, rt2, Text.equal, E.attributeMapsEqual);
  };

  /// Mostly for testing/debugging purposes, generates a textual representation of the RangeTree
  public func toText(rt: RangeTree): Text.Text {
    switch(rt) {
      case (#leaf) { "#leaf" };
      case (#node(c, l, (sk, attrs), r)) {
        let color = switch(c) {
          case (#R) { "#R" };
          case (#B) { "#B" }
        };
        let attributeMap = switch(attrs) {
          case null { "null" };
          case (?map) { E.attributeMapToText(map) }
        };
        "#node(color=" # color # 
        ", l=" # toText(l) 
        # ", {sk=" # sk # ", attributeMap={" # attributeMap # "}, r=" # toText(r) # ")";
      };
    }
  };

  func updateRoot(rt: RangeTree, sk : E.SK, updateFn: (?E.AttributeMap) -> E.AttributeMap): (?E.AttributeMap, RangeTree) {
    switch (updateRec(rt, sk, updateFn)) {
      case (_, #leaf) { assert false; loop { } };
      case (vo, #node(_, l, kv, r)) { (vo, #node(#B, l, kv, r)) };
    }
  };

  func updateRec(rt: RangeTree, sk : E.SK, updateFn: (?E.AttributeMap) -> E.AttributeMap): (?E.AttributeMap, RangeTree) {
    switch rt {
      case (#leaf) { (null, #node(#R, #leaf, (sk, ?updateFn(null)), #leaf)) };
      case (#node(c, l, (k, v), r)) {
        switch (Text.compare(sk, k)) {
          case (#less) {
            let (vo, l2) = updateRec(l, sk, updateFn);
            (vo, bal<E.SK, E.AttributeMap>(c, l2, (k, v), r))
          };
          case (#equal) {
            (v, #node(c, l, (k, ?updateFn(v)), r))
          };
          case (#greater) {
            let (vo, r2) = updateRec(r, sk, updateFn);
            (vo, bal<E.SK, E.AttributeMap>(c, l, (k, v), r2))
          };
        }
      }
    }
  };

  // Adapted directly from RBTree.mo in motoko-base (rebalances the current node and its children)
  func bal<K, V>(color : RBT.Color, lt : RBT.Tree<K, V>, kv : (K, ?V), rt : RBT.Tree<K, V>) : RBT.Tree<K, V> {
    // thank you, algebraic pattern matching!
    // following notes from [Ravi Chugh](https://www.classes.cs.uchicago.edu/archive/2019/spring/22300-1/lectures/RedBlackTrees/index.html)
    switch (color, lt, kv, rt) {
      case (#B, #node(#R, #node(#R, a, k, b), v, c), z, d) {
        #node(#R, #node(#B, a, k, b), v, #node(#B, c, z, d))
      };
      case (#B, #node(#R, a, k, #node(#R, b, v, c)), z, d) {
        #node(#R, #node(#B, a, k, b), v, #node(#B, c, z, d))
      };
      case (#B, a, k, #node(#R, #node(#R, b, v, c), z, d)) {
        #node(#R, #node(#B, a, k, b), v, #node(#B, c, z, d))
      };
      case (#B, a, k, #node(#R, b, v, #node(#R, c, z, d))) {
        #node(#R, #node(#B, a, k, b), v, #node(#B, c, z, d))
      };
      case _ { #node(color, lt, kv, rt) };
    }
  };

  type RangeTreeNode = { #node: (RBT.Color, RangeTree, (E.SK, ?E.AttributeMap), RangeTree)};

  func iterScanLimit(rt: RangeTree, lower: E.SK, upper: E.SK, limit: Nat, dir: Direction): ([(E.SK, E.AttributeMap)], ?E.SK) {
    var remaining = limit + 1;
    let resultBuffer: Buffer.Buffer<(E.SK, E.AttributeMap)> = Buffer.Buffer(0);
    var nextKey: ?E.SK = null;
    var nodeStack = Stack.Stack<RangeTreeNode>();
    var currentNode = rt;

    while (remaining > 0) {
      // this loop finds the next non-deleted node in order, adds it to the stack if that node exists, and then exits the loop 
      // otherwise a leaf is hit and the loop is exited 
      label l loop {
        switch(currentNode) {
          case (#node(c, l, (sk, map), r)) {
            // compare the node to see if it is within the sk bounds, if so, add it to the nodeStack
            switch(dir, Text.compare(sk, lower), Text.compare(sk, upper)) {
              // value is greater than lower and upper bounds, traverse left child regardless of direction order
              case (_, #greater, #greater) {
                currentNode := l;
              };
              // if ascending order and value is greater than lower and equal to upper, push node to stack and go left
              case (#fwd, #greater, #equal) {
                nodeStack.push(#node(c, l, (sk, map), r));
                currentNode := l 
              };
              // if descending order and value is greater than lower and equal to upper
              case (#bwd, #greater, #equal) {
                // if attribute map is not null, push to stack and break as can not go any farther to the right 
                if (Option.isSome(map)) {
                  nodeStack.push(#node(c, l, (sk, map), r));
                  break l;
                // otherwise go left, as the current node was deleted and can still go to the left 
                } else {
                  currentNode := l;
                }
              };
              // value is greater than lower and less than upper, push node to stack, then traverse left or right child depending on direction order
              case (#fwd, #greater, #less) {
                nodeStack.push(#node(c, l, (sk, map), r));
                currentNode := l; 
              };
              case (#bwd, #greater, #less) {
                nodeStack.push(#node(c, l, (sk, map), r));
                currentNode := r;
              };
              // if ascending order and value is equal to lower and less than upper
              case (#fwd, #equal, #less) {
                // if attribute map is not null, push to stack and break as can not go any farther to the left 
                if (Option.isSome(map)) {
                  nodeStack.push(#node(c, l, (sk, map), r));
                  break l;
                // otherwise go right, as the current node was deleted and can still go to the right
                } else {
                  currentNode := r;
                }
              };
              // if descending order and value is equal to lower and less than upper
              case (#bwd, #equal, #less) {
                nodeStack.push(#node(c, l, (sk, map), r));
                currentNode := r;
              };
              // if value is less than lower and upper bounds, traverse right child regardless of direction order
              case (_, #less, #less) {
                currentNode := r;
              };
              // This should never be hit as the cases where lower bound >= upper bound are covered in the main scan function
              case _ { 
                break l;
              };
            }
          };
          // have already hit the next node in order, exit the loop
          case (#leaf) { 
            break l;
          };
        }
      };

      // pop the next node from the stack
      switch(nodeStack.pop()) {
        // if the stack is empty, no more nodes within the bounds exist, so can return
        case null { 
          return (Buffer.toArray(resultBuffer), nextKey);
        };
        case (?(#node(_, l, (sk, map), r))) {
          switch(map) {
            // if the popped node's map is null (was deleted), skip it and traverse to the right child
            case null {};
            // if the popped node's map is present, prepend it to the entries list and traverse to the right child
            case (?attributeMap) {
              if (remaining == 1) {
                nextKey := ?sk;
              } else {
                resultBuffer.add((sk, attributeMap));
              };
              remaining -= 1;
            }
          };
          // traverse to the left or right child depending on the direction order
          currentNode := switch(dir) {
            case (#fwd) { r };
            case (#bwd) { l };
          };
        }
      }
    };

    return (Buffer.toArray(resultBuffer), nextKey);
  };

  type IterScanRep = List.List<{ #rt: RangeTree; #kv: (E.SK, ?E.AttributeMap)}>;

  func iterScan(rt: RangeTree, lower: E.SK, upper: E.SK): I.Iter<(E.SK, E.AttributeMap)> {
    object {
      var trees: IterScanRep = ?(#rt(rt), null); 
      public func next() : ?(E.SK, E.AttributeMap) {
        switch(trees) {
          case null { null };
          case (?(#rt(#leaf), rts)) {
            trees := rts;
            next()
          };
          case (?(#kv((sk, attributeMap)), ts)) {
            trees := ts;
            switch (attributeMap) {
              case null { next() };
              case (?map) { ?(sk, map) }
            }
          };
          case (?(#rt(#node(_, l, (sk, attributeMap), r)), rts)) {
            trees := rtAddIfInRange(rts, l, r, (sk, attributeMap));
            next();
          }
        }
      };

      func rtAddIfInRange(rts: IterScanRep, l: RangeTree, r: RangeTree, (sk: E.SK, map: ?E.AttributeMap)): IterScanRep {
        switch(Text.compare(sk, lower), Text.compare(sk, upper)) {
          // value is greater than lower and upper bounds, go left
          case (#greater, #greater) {
            ?(#rt(l), rts);
          };
          // value is greater than lower and equal to upper, go left then append map to tail
          case (#greater, #equal) {
            ?(#rt(l), ?(#kv((sk, map)), rts));
          };
          // value is greater than lower and less than upper, go left, append map, and go right 
          case (#greater, #less) {
            ?(#rt(l), ?(#kv((sk, map)), ?(#rt(r), rts)));
          };
          // value is equal to lower and less than upper, prepend map and go right
          case (#equal, #less) {
            ?(#kv((sk, map)), ?(#rt(r), rts));
          };
          // value is less than lower and upper bounds, go right
          case (#less, #less) {
            ?(#rt(r), rts);
          };
          // the cases where lower bound >= upper bound are covered in the main scan function
          case _ { rts }
        }
      }
    }
  }
}