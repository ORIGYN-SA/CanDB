import Text "mo:base/Text";

import E "../src/Entity";
import HT "../src/HashTree";

/// This module contains helpers for setting up data structures in tests
module {
  // Test helper that creates mock attributes
  public func createMockAttributes(city: Text): E.AttributeMap {
    let attributes = [
      ("state", #text("OH")),
      ("year", #int(2020)),
      ("city", #text(city))
    ];

    E.createAttributeMapFromKVPairs(attributes);
  };

  // Test helper that creates a Hash Tree with the specified pk, sk, and fixed attributes
  public func createHashTreeWithPKSKMockEntries(pksks: [(E.PK, E.SK)]): HT.HashTree {
    let ht = HT.init();
    let mockAttributes = createMockAttributes("Cleveland");
    for ((pk, sk) in pksks.vals()) {
      HT.put(ht, {
        pk = pk;
        sk = sk;
        attributes = mockAttributes;
      })
    };
    ht;
  };

  // Helper function for the update() function used to demonstrate how its updateFunction parameter is used
  // increments the count attribute, or sets it to 1 if does not exist
  public func incrementFunc(attributeMap: ?E.AttributeMap): E.AttributeMap {
    let countKey = "count";
    switch(attributeMap) {
      case null { 
        E.createAttributeMapFromKVPairs([
          (countKey, #int(1))
        ])
      };
      case (?map) {
        switch(E.getAttributeMapValueForKey(map, countKey)) {
          case null {
            let updated = E.updateAttributeMapWithKVPairs(map, [
              (countKey, #int(1)),
              ("isCountNull", #bool(false))
            ]);
            updated;
          };
          case (?(#int(existingCount))) {
            E.updateAttributeMapWithKVPairs(map, [
              (countKey, #int(existingCount + 1)),
              ("isCountNull", #bool(false))
            ])
          };
          case _ { 
            // count is of wrong type, not updating
            map
          }
        }
      }
    }
  };
}