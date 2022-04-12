import E "../src/Entity";
import HT "../src/HashTree";
import Text "mo:base/Text";

/// This module contains helpers for setting up data structures in tests
module {
  // Test helper that creates mock attributes
  public func createMockAttributes(city: Text): E.AttributeMap {
    let attributes = [
      ("state", #Text("OH")),
      ("year", #Int(2020)),
      ("city", #Text(city))
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
}