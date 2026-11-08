#!/usr/bin/env node
// Pack one publishable workspace package (lineage-client or lineage-ui) into a
// tarball, the way .github/workflows/publish-npm.yml publishes it:
//
//   node scripts/pack-package.mjs <package-dir> <out-dir>
//
// Run from node/ after `npm run build` for the package (and its workspace
// dependencies). The version comes from the package's release-plz placeholder
// crate (<package-dir>/Cargo.toml), which is the npm version; package.json
// carries 0.0.0. A workspace dependency (`"*"`) is pinned to `^<its version>`
// the same way. The manifest is restored afterwards, so this is safe locally.
// Prints `{ "version", "tarball" }` as JSON on the last line of stdout.

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const [dir, outDir] = process.argv.slice(2);
if (!dir || !outDir || !/^[a-z][a-z0-9-]*$/.test(dir)) {
  console.error("usage: pack-package.mjs <package-dir> <out-dir>");
  process.exit(2);
}

/** The `version` of the placeholder crate in `pkgDir`. */
function crateVersion(pkgDir) {
  const manifest = readFileSync(join(pkgDir, "Cargo.toml"), "utf8");
  const match = manifest.match(/^version = "([^"]+)"$/m);
  if (!match) throw new Error(`no version in ${pkgDir}/Cargo.toml`);
  return match[1];
}

/** Workspace package name → its directory, for pinning `"*"` dependencies. */
const workspaceDirs = new Map();
for (const ws of JSON.parse(readFileSync("package.json", "utf8")).workspaces) {
  const { name } = JSON.parse(readFileSync(join(ws, "package.json"), "utf8"));
  workspaceDirs.set(name, ws);
}

const pkgPath = join(dir, "package.json");
const original = readFileSync(pkgPath, "utf8");
const pkg = JSON.parse(original);
const version = crateVersion(dir);

if (!existsSync(join(dir, "dist", "index.js"))) {
  console.error(`${dir}/dist is not built; run \`npm run build -w ${pkg.name}\``);
  process.exit(1);
}

pkg.version = version;
const problems = [];
for (const section of ["dependencies", "peerDependencies"]) {
  for (const [name, range] of Object.entries(pkg[section] ?? {})) {
    if (range !== "*") continue;
    const depDir = workspaceDirs.get(name);
    if (!depDir || !existsSync(join(depDir, "Cargo.toml"))) {
      problems.push(`${section}.${name} is "*" and not a publishable workspace package`);
      continue;
    }
    pkg[section][name] = `^${crateVersion(depDir)}`;
  }
}
if (problems.length > 0) {
  console.error(`pack-package: ${problems.join("\n  ")}`);
  process.exit(1);
}

let tarball;
try {
  writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
  const out = execFileSync(
    "npm",
    ["pack", "--json", "--pack-destination", resolve(outDir)],
    { cwd: dir, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] },
  );
  const [info] = JSON.parse(out);
  tarball = join(resolve(outDir), info.filename);
  const files = info.files.map((f) => f.path);
  for (const required of ["package.json", "dist/index.js", "dist/index.d.ts"]) {
    if (!files.includes(required)) problems.push(`the tarball lacks ${required}`);
  }
  for (const f of files) {
    if (/\.(test|stories)\.tsx?$/.test(f) || f.startsWith("src/testing/")) {
      problems.push(`the tarball ships ${f}`);
    }
  }
} finally {
  writeFileSync(pkgPath, original);
}
if (problems.length > 0) {
  console.error(`pack-package: ${problems.join("\n  ")}`);
  process.exit(1);
}
console.log(JSON.stringify({ version, tarball }));
