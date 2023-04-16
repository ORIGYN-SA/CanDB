import { exec } from "child_process";
import fs from "fs-extra";
import glob from "glob";

/**
 * Generates TS declarations using child process to run dfx.
 */
async function generate() {
  return new Promise((res) => exec("dfx generate", res));
}

/**
 * Clean up the generated declarations.
 */
async function clean() {
  if (await fs.pathExists("src")) await fs.rm("src", { recursive: true });
}

/**
 * Modifies the generated declarations to be more usable.
 */
async function prepare() {
  await new Promise((res) =>
    glob("src/declarations/**/index.js", async (_, files) => {
      await Promise.all(files.map(async (file) => await fs.rm(file)));
      res(null);
    })
  );
  await new Promise((res) =>
    glob("src/declarations/**/*.did", async (_, files) => {
      await Promise.all(files.map(async (file) => await fs.rm(file)));
      res(null);
    })
  );
  await new Promise((res) =>
    glob("src/declarations/**/*.js", async (_, files) => {
      await Promise.all(
        files.map(async (file) => {
          let contents = await fs.readFile(file, "utf8");
          contents = contents.replace(
            "export const idlFactory = ({ IDL }) => {",
            'import { IDL } from "@dfinity/candid";\nexport const idlFactory : IDL.InterfaceFactory = ({ IDL }) => {'
          );
          contents = contents.replace(
            /export const init = \(\{ IDL \}\) => \{ return \[(.+)?\]; \};/,
            ""
          );
          await fs.writeFile(file, contents);
          await fs.move(file, file.replace(".js", ".ts"), { overwrite: true });
        })
      );
      res(null);
    })
  );
}

export async function run() {
  await clean();
  await generate();
  await prepare();
  await fs.copy("src/declarations", "tests/declarations", {
    recursive: true,
    overwrite: true,
  });
  await clean();
}

run();