/// InterfaceSpec - The IC Management Canister Interface Spec

import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";

module {
  // Types adapted from the interface spec https://github.com/dfinity/interface-spec/blob/master/spec/ic.did
  public type CanisterSettings = {
    controllers : ?[Principal];
    compute_allocation : ?Nat;
    memory_allocation : ?Nat;
    freezing_threshold : ?Nat;
  };

  public type DefiniteCanisterSettings = {
    controllers : [Principal];
    compute_allocation : Nat;
    memory_allocation : Nat;
    freezing_threshold : Nat;
  };

  // Methods adapted from the interface spec https://github.com/dfinity/interface-spec/blob/master/spec/ic.did
  // "aaaaa-aa" is the management canister
  // public let IC = actor "aaaaa-aa" : actor {
  public type IC = actor { // let IC = actor {
    create_canister : shared { settings : ?CanisterSettings } -> async { canister_id : Principal };
    update_settings : shared { canister_id : Principal; settings : CanisterSettings } -> async ();
    install_code : shared {
      mode : { #install; #reinstall; #upgrade};
      canister_id : Principal;
      wasm_module : Blob;
      arg : Blob;
    } -> async ();
    uninstall_code : shared { canister_id : Principal } -> async ();
    start_canister : shared { canister_id : Principal } -> async ();
    stop_canister : shared { canister_id : Principal } -> async ();
    canister_status : shared { canister_id : Principal } -> async {
        status : { #running; #stopping; #stopped };
        settings: DefiniteCanisterSettings;
        module_hash: ?Blob;
        memory_size: Nat;
        cycles: Nat;
    };
    delete_canister : { canister_id : Principal } -> async ();
    deposit_cycles : { canister_id : Principal } -> async ();
    raw_rand : () -> async Blob;

    // provisional interfaces for the pre-ledger world
    provisional_create_canister_with_cycles : shared {
      amount: ?Nat;
      settings : ?CanisterSettings
    } -> async { canister_id : Principal };
    provisional_top_up_canister : shared { canister_id : Principal; amount : Nat } -> async ();
  }
}