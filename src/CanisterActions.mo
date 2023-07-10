/// CanisterActions - High level asynchronous functions for interacting with the IC Management Canister

import Cycles "mo:base/ExperimentalCycles";
import Debug "mo:base/Debug";
import Nat "mo:base/Nat";

import InterfaceSpec "./InterfaceSpec";

module {
  let ic: InterfaceSpec.IC = actor "aaaaa-aa";

  /// Calls the ic management canister's update_settings method with 
  /// the provided parameters to update the canister's settings
  /// belonging to the provided canister principal 
  public func updateCanisterSettings({
    canisterId: Principal;
    settings: InterfaceSpec.CanisterSettings;
  }): async () {
    await ic.update_settings({
      canister_id = canisterId;
      settings = settings;
    })
  };

  /// Calls the ic management canister's install_code method with 
  /// `mode = #upgrade` and the provided parameters to upgrade 
  /// the canister belonging to the provided canister principal 
  public func upgradeCanisterCode({
    canisterId: Principal; 
    wasmModule: Blob; 
    args: Blob;
  }): async () {
    await ic.install_code({
      arg = args; 
      wasm_module = wasmModule;
      mode = #upgrade;
      canister_id = canisterId;
    });
  };

  /// Calls the ic management canister's delete_canister method to stop 
  /// the canister belonging to the provided canister principal 
  public func stopCanister(canisterPrincipal: Principal): async () {
    await ic.stop_canister({ canister_id = canisterPrincipal });
  };

  /// Calls the ic management canister's delete_canister method to delete
  /// the canister belonging to the provided canister principal 
  public func deleteCanister(canisterPrincipal: Principal): async () {
    await ic.delete_canister({ canister_id = canisterPrincipal });
  };

  /// Calls the ic management canister's deposit_cycles method to transfer cycles
  /// back to the provided canisterId principal
  public func transferCycles(transferToCanisterPrincipal: Principal): async () {
    let balance: Nat = Cycles.balance() - 100_000_000_000;
    // TODO: look into returning some type of error if this is negative?
    if (balance > 0) { 
      try {
        Cycles.add(balance);
        await ic.deposit_cycles({ canister_id = transferToCanisterPrincipal });
      } catch(error) {
        // usually this error is because the canister's cycles are below the freezing threshold if it does this
        // for now just have it try one more time leaving 200_000_000 cycles - otherwise give up and continue 
        // (cycles could be deleted - need to find a fix for this issue)
        let try2Balance = balance + 100_000_000;
        Cycles.add(balance);
        await ic.deposit_cycles({ canister_id = transferToCanisterPrincipal });
      }
    }
  };
}