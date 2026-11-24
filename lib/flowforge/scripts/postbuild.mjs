import { mkdirSync, writeFileSync } from "node:fs";

mkdirSync(new URL("../dist/esm", import.meta.url), { recursive: true });
mkdirSync(new URL("../dist/cjs", import.meta.url), { recursive: true });
writeFileSync(new URL("../dist/esm/package.json", import.meta.url), '{"type":"module"}\n');
writeFileSync(new URL("../dist/cjs/package.json", import.meta.url), '{"type":"commonjs"}\n');
