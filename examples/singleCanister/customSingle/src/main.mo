/// Custom single canister example of a barebones actor backend that directly using CanDB for CRUD functionality with Candy 2.0
///
/// Example shows the following
/// 1. Initialization of CanDB inside an actor
/// 2. Several Basic APIs: 
///     get() 
///     update() - (i.e. put)
///     remove() - (i.e. delete)
///     scan()
/// 3. Several APIs dervied from the updateAttributeMapFunction parameter in CanDB.update() 
///     update() 

import Array "mo:base/Array";
import Text "mo:base/Text";

import CandyTypes "mo:candy/types";

import CanDB "../../../../src/SingleCanisterCanDB";
import Entity "../../../../src/Entity";

actor {
    // initializes an instance of CanDB - yes, that's all you need!
    stable let db = CanDB.init();

    // meta field uses CandyTypes from Candy 2.0 library
    type User = {
       name: Text;
       year: Int;
       meta: CandyTypes.CandyShared;
    };

    public func create(user: User) : async () {
        CanDB.put(db, {
            pk = "origyn"; 
            sk = user.name;
            attributes = [
                ("name", #text(user.name)),
                ("year", #int(user.year)),
                ("meta", #candy(user.meta))
            ]
        });

     ()
    };

    public query func get(name: Text): async ?User {
        let userData = switch(CanDB.get(db, { pk= "origyn"; sk = name }))  {
            case null { null };
            case (?userEntity) { unwrapUser(userEntity)};
        };

        switch(userData) {
            case(?u) ? { name = u.name; year = u.year; meta = u.meta };
            case null { null };
        };
        
    };

    public func update(
        { pk: Entity.PK; 
          sk: Entity.SK; 
          attributesToUpdate: [(Entity.AttributeKey, Entity.AttributeValue)]; 
        }
    ): async ?User {

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
            case (?entity) { unwrapUser(entity) }
        };
    };

    public func remove(options: CanDB.RemoveOptions): async ?User {
        switch(CanDB.remove(db, options)) {
            case null { null };
            case (?entity) { unwrapUser(entity) }
        }
    };

    public query func scan(options: CanDB.ScanOptions): async [?User] {

        let { entities; nextKey } = CanDB.scan(db, options);
            Array.map<Entity.Entity, ?User>(entities, func(entity): ?User {
                unwrapUser(entity);
            });
    };

    func unwrapUser(entity: Entity.Entity): ?User {

        let { sk; pk; attributes } = entity;
        let nameValue = Entity.getAttributeMapValueForKey(attributes, "name");
        let yearValue = Entity.getAttributeMapValueForKey(attributes, "year");
        let metaValue = Entity.getAttributeMapValueForKey(attributes, "meta");

        switch(nameValue, yearValue, metaValue) {
            case (
                ?(#text(name)),
                ?(#int(year)),
                ?(#candy(meta)),
            ) { ? { name; year; meta; } };
            case _ { null };
        };
    };


}