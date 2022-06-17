import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import I "mo:base/Iter";
import Buffer "mo:stable-buffer/StableBuffer";
import Entity "../../../src/Entity";


module {
  public type CanisterIdList = Buffer.StableBuffer<Text>;
  public type CanisterMap = RBT.Tree<Entity.PK, CanisterIdList>; 

  public func init(): CanisterMap { RBT.init<Entity.PK, CanisterIdList>() };

  public func get(map: CanisterMap, pk: Entity.PK): ?CanisterIdList {
    RBT.get<Text, CanisterIdList>(map, Text.compare, pk);
  };

  public func add(map: CanisterMap, pk: Entity.PK, canisterId: Text): CanisterMap {
    func appendToOrCreateBuffer(existingCanisterIdsForPK: ?CanisterIdList): CanisterIdList {
      let canisterIdsBuffer = switch(existingCanisterIdsForPK) {
        case null { Buffer.initPresized<Text>(1) };
        case (?canisterIdsBuffer) { canisterIdsBuffer }
      };
      Buffer.add<Text>(canisterIdsBuffer, canisterId);
      canisterIdsBuffer;
    };
    let (_, newMap) = RBT.update<Entity.PK, CanisterIdList>(map, Text.compare, pk, appendToOrCreateBuffer);
    newMap;
  };

  public func delete(map: CanisterMap, pk: Entity.PK): CanisterMap {
    RBT.delete<Entity.PK, CanisterIdList>(map, Text.compare, pk);
  };

  public func entries(map: CanisterMap): I.Iter<(Entity.PK, CanisterIdList)> { RBT.entries(map) }; 
}