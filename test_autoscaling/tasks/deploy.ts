import { execSync } from "child_process";

async function deploy() {
  execSync(`echo "yes" | dfx deploy index`);
  // need to create a canister to generate declartions
  execSync(`dfx canister create testService`)
}

deploy();