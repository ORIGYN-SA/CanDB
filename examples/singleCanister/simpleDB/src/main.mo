/// Simple single canister example of a barebones actor backend that directly using CanDB for CRUD functionality
///
/// Example shows the following
/// 1. Initialization of CanDB inside an actor
/// 2. Several Basic APIs: 
///     get() 
///     replace() - (i.e. put)
///     remove() - (i.e. delete)
///     scan()
/// 3. Several APIs dervied from the updateAttributeMapFunction parameter in CanDB.update() 
///     create() 
///     update() 

import CanDB "../../../src/SingleCanisterCanDB";
import Entity "../../../src/Entity";
import Array "mo:base/Array";

actor {

  // initializes an instance of CanDB - yes, that's all you need!
  stable let db = CanDB.init();

  type ConsumableEntity = {
    pk: Entity.PK;
    sk: Entity.SK;
    attributes: [(Entity.AttributeKey, Entity.AttributeValue)];
  };

  // The following are a few examples of how you might interact with CanDB in your app

  /// Creates an entity if that entity does not already exist. Returns the entity if creation was
  /// successful and returns null if the entity already existed
  public func create(entity: ConsumableEntity): async ?ConsumableEntity {
    let createAttributes = Entity.createAttributeMapFromKVPairs(entity.attributes);
    // Developer defined function passed to CanDB.update() that mimics create functionality by updating
    // the AttributeMap of the entity if that entity does not yet exist, but preserves the original 
    // AttributeMap if it already exists 
    func createAttributesIfEntityDoesNotExist(attributeMap: ?Entity.AttributeMap): Entity.AttributeMap {
      switch(attributeMap) {
        case null { createAttributes };
        case (?map) { map };
      };
    };

    switch(CanDB.update(db, { 
      pk = entity.pk; 
      sk = entity.sk;
      updateAttributeMapFunction = createAttributesIfEntityDoesNotExist;
    })) {
      // creation was successful
      case null { 
        ?{
          pk = entity.pk;
          sk = entity.sk; 
          attributes = entity.attributes;
        }
      };
      // entity already existed
      case (?entity) { null }
    };
  };

  /// Get an entity if already exists
  /// Provides an example of how one would transform the AttributeMap returned with the Entity
  /// to a list of Attribute Key Value pairs, using the extractKVPairsFromAttributeMap function
  public query func get(options: CanDB.GetOptions): async ?ConsumableEntity {
    switch(CanDB.get(db, options)) {
      case null { null };
      case (?entity) {
        ?{
          pk = entity.pk;
          sk = entity.sk;
          attributes = Entity.extractKVPairsFromAttributeMap(entity.attributes);
        }
      }
    }
  };

  /// Replace an entity if already exists, otherwise create an entity. Returns the previous entity if it existed
  public func replace(options: CanDB.ReplaceOptions): async ?ConsumableEntity {
    switch(CanDB.replace(db, options)) {
      case null { null };
      case (?entity) {
        ?{
          pk = entity.pk;
          sk = entity.sk;
          attributes = Entity.extractKVPairsFromAttributeMap(entity.attributes);
        }
      }
    }
  };

  /// Update specific attributes of an entity if already exists, otherwise create a new entity. 
  /// Returns the previous entity if it existed
  public func update({ 
    pk: Entity.PK; 
    sk: Entity.SK; 
    attributesToUpdate: [(Entity.AttributeKey, Entity.AttributeValue)]; 
  }): async ?ConsumableEntity {
    // Developer defined function passed to CanDB.update() that mimics an expected update() functionality by 
    // updating the specific attributes of the AttributeMap of the entity if that entity exists, but creates
    // a new entity with the passed attributes if that entity does not yet exist
    func updateAttributes(attributeMap: ?Entity.AttributeMap): Entity.AttributeMap {
      switch(attributeMap) {
        case null { Entity.createAttributeMapFromKVPairs(attributesToUpdate) };
        case (?map) { Entity.updateAttributeMapWithKVPairs(map, attributesToUpdate) }
      }
    };

    switch(CanDB.update(db, { 
      pk = pk; 
      sk = sk;
      updateAttributeMapFunction = updateAttributes;
    })) {
      case null { null };
      case (?entity) {
        ?{
          pk = entity.pk;
          sk = entity.sk;
          attributes = Entity.extractKVPairsFromAttributeMap(entity.attributes); 
        }
      }
    }
  };

  /// Removes an entity from the DB if exists. Returns the removed entity
  public func remove(options: CanDB.RemoveOptions): async ?ConsumableEntity {
    switch(CanDB.remove(db, options)) {
      case null { null };
      case (?entity) {
        ?{
          pk = entity.pk;
          sk = entity.sk;
          attributes = Entity.extractKVPairsFromAttributeMap(entity.attributes);
        }
      }
    }
  };

  /// Scans the DB by partition key, a lower/upper bounded sort key range, and a desired result limit
  /// Returns 0 or more items from the db matching the conditions of the ScanOptions passed
  public query func scan(options: CanDB.ScanOptions): async { entities: [ConsumableEntity]; nextKey: ?Entity.SK } {
    let { entities; nextKey } = CanDB.scan(db, options);
    {
      entities = Array.map<Entity.Entity, ConsumableEntity>(entities, func(entity): ConsumableEntity {
        {
          pk = entity.pk;
          sk = entity.sk;
          attributes = Entity.extractKVPairsFromAttributeMap(entity.attributes);
        }
      });
      nextKey = nextKey;
    }
  };
}