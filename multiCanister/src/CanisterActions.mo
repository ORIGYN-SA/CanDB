import InterfaceSpec "./InterfaceSpec";
import Cycles "mo:base/ExperimentalCycles";
import Debug "mo:base/Debug";

module {
  let ic: InterfaceSpec.IC = actor "aaaaa-aa";

  public func updateCanisterSettings({
    canisterId: Principal;
    settings: InterfaceSpec.CanisterSettings;
  }): async () {
    await ic.update_settings({
      canister_id = canisterId;
      settings = settings;
    })
  };

  public func upgradeCanisterCode(ic: InterfaceSpec.IC, canisterId: Principal, wasmModule: Blob, args: Blob): async () {
    await ic.install_code({
      arg = args; 
      wasm_module = wasmModule;
      mode = #upgrade;
      canister_id = canisterId;
    });
  };

  public func stopCanister(canisterId: Principal): async Text {
    await ic.stop_canister({ canister_id = canisterId });
    "done";
  };

  public func deleteCanister(canisterId: Principal): async Text {
    await ic.delete_canister({ canister_id = canisterId });
    "done";
  };

  public func depositCycles(canisterId: Principal): async () {
    await ic.deposit_cycles({ canister_id = canisterId });
  };

  public func transferCycles(transferTo: Principal): async Text {
    let balance: Nat = Cycles.balance() - 100_000_000_000;
    Debug.print("added balance = " # debug_show(balance));
    Cycles.add(balance);
    await ic.deposit_cycles({ canister_id = transferTo });
    "done";
  }
}