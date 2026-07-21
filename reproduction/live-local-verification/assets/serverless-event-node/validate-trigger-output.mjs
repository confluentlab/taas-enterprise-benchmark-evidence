import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const [fixtureRoot, bundleRoot, simulationPath] = process.argv.slice(2);
const readJsonl = (file) => fs.readFileSync(file, "utf8").trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
const sort = (value) => Array.isArray(value) ? value.map(sort) : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, sort(value[key])])) : value;
const hash = (value) => crypto.createHash("sha256").update(JSON.stringify(sort(value))).digest("hex");
const expected = simulationPath
  ? JSON.parse(fs.readFileSync(simulationPath, "utf8")).records.map((record) => record.outputPreview)
  : readJsonl(path.join(fixtureRoot, "expected-valid-output.jsonl"));
const transformed = readJsonl(path.join(bundleRoot, "actual", "transformed-output.jsonl"));
const errors = readJsonl(path.join(bundleRoot, "actual", "error-output.jsonl"));
const expectedHashes = expected.map(hash).sort(), actualHashes = transformed.map(hash).sort();
const duplicates = transformed.length - new Set(actualHashes).size;
const expectedHashMatches = JSON.stringify(expectedHashes) === JSON.stringify(actualHashes) ? expected.length : 0;
const errorContractMatches = errors.filter((value) => value?.error?.code === "MULTIPLE_FIELD_ERRORS" && Array.isArray(value.errors) && value.errors.length > 0).length;
const report = { expectedOutputs: expected.length, actualOutputs: transformed.length, expectedHashMatches, intentionalErrors: errors.length, errorContractMatches, duplicates, passed: expectedHashMatches === expected.length && errors.length === 10 && errorContractMatches === 10 && duplicates === 0 };
fs.writeFileSync(path.join(bundleRoot, "actual", "content-validation.json"), `${JSON.stringify(report, null, 2)}\n`);
if (!report.passed) throw new Error(`trigger content validation failed: ${JSON.stringify(report)}`);
console.log(JSON.stringify(report));
